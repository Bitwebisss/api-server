# server/electrum.py
"""
ElectrumX JSON-RPC client over TLS.

- ElectrumClient     — single thread-safe connection with auto-reconnect
- ElectrumPool       — fixed pool of N clients, round-robin via queue
- ElectrumSubscriber — dedicated connection for server-push notifications
"""

import json
import logging
import queue
import socket
import ssl
import threading
import time

log = logging.getLogger(__name__)

CLIENT_NAME    = "BitwebWallet"
CLIENT_VERSION = "1.0"
PROTOCOL_MIN   = "1.4"
PROTOCOL_MAX   = "1.4"

# ElectrumX closes idle connections after ~10 minutes at the protocol level,
# but the observed server-side idle timeout is ~240 s.
# Ping every 3 minutes (180 s) to stay safely within that window.
PING_INTERVAL = 180  # seconds


# ---------------------------------------------------------------------------
# ElectrumClient
# ---------------------------------------------------------------------------

class ElectrumClient:
    """Thread-safe ElectrumX connection. Reconnects once on socket error."""

    def __init__(self, host, port, timeout=15, verify_ssl=True):
        self.host       = host
        self.port       = port
        self.timeout    = timeout
        self.verify_ssl = verify_ssl
        self._sock      = None
        self._buf       = b""
        self._req_id    = 0
        self._lock      = threading.Lock()

    def _ssl_context(self):
        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode    = ssl.CERT_NONE
        return ctx

    def _connect(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = None
        self._buf  = b""
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(self.timeout)
        raw.connect((self.host, self.port))
        self._sock = self._ssl_context().wrap_socket(raw, server_hostname=self.host)
        log.info("Connected to %s:%s", self.host, self.port)
        self._rpc("server.version", [f"{CLIENT_NAME} {CLIENT_VERSION}", [PROTOCOL_MIN, PROTOCOL_MAX]])

    def _rpc(self, method, params):
        self._req_id += 1
        req_id = self._req_id
        self._sock.sendall((json.dumps({"id": req_id, "method": method, "params": params}) + "\n").encode())
        while True:
            while b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                resp = json.loads(line)
                if resp.get("id") != req_id:
                    continue  # server-push notification, not our response
                if resp.get("error"):
                    raise RuntimeError(f"ElectrumX error: {resp['error']}")
                return resp["result"]
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("Server closed the connection")
            self._buf += chunk

    def call(self, method, *params):
        with self._lock:
            try:
                if not self._sock:
                    self._connect()
                return self._rpc(method, list(params))
            except (OSError, ConnectionError, ssl.SSLError) as exc:
                log.warning("Socket error on %s (%s) — reconnecting", method, exc)
                self._connect()
                return self._rpc(method, list(params))

    def close(self):
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
            self._buf = b""


# ---------------------------------------------------------------------------
# ElectrumPool
# ---------------------------------------------------------------------------

class ElectrumPool:
    """Round-robin pool of ElectrumClient connections."""

    def __init__(self, host, port, timeout, verify_ssl, size=8):
        self._queue = queue.Queue()
        for _ in range(size):
            self._queue.put(ElectrumClient(host, port, timeout, verify_ssl))

    def call(self, method, *params):
        client = self._queue.get()
        try:
            return client.call(method, *params)
        finally:
            self._queue.put(client)

    def close(self):
        while not self._queue.empty():
            try:
                self._queue.get_nowait().close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# ElectrumSubscriber
# ---------------------------------------------------------------------------

class ElectrumSubscriber:
    """
    Persistent connection for server-push notifications.

    Keepalive strategy
    ------------------
    ElectrumX closes idle connections after ~10 minutes at the application
    level. TCP keepalive does NOT prevent this — it only detects dead TCP
    stacks. The real fix is an application-level ping:

      - recv runs with settimeout(PING_INTERVAL)
      - on socket.timeout  → send server.ping
      - ElectrumX replies  {"id": N, "result": null}; _dispatch ignores it
        (no "method" field)
      - if send/recv raises OSError the outer _run loop reconnects as usual
    """

    def __init__(self, host, port, timeout, verify_ssl):
        self.host       = host
        self.port       = port
        self.timeout    = timeout
        self.verify_ssl = verify_ssl

        self._sock      = None
        self._buf       = b""
        self._req_id    = 0
        self._send_lock = threading.Lock()

        self._subscribed = set()
        self._sub_lock   = threading.Lock()

        self._running = False

        # Assign before calling start()
        self.on_new_block         = None  # callable(height: int)
        self.on_scripthash_change = None  # callable(scripthash: str)

    def start(self):
        self._running = True
        t = threading.Thread(target=self._run, daemon=True, name="electrum-subscriber")
        t.start()

    def subscribe_scripthash(self, scripthash):
        """Idempotent, thread-safe."""
        with self._sub_lock:
            if scripthash in self._subscribed:
                return
            self._subscribed.add(scripthash)
        if self._sock:
            self._send("blockchain.scripthash.subscribe", [scripthash])

    # ------------------------------------------------------------------
    # Internal

    def _run(self):
        while self._running:
            try:
                self._connect()
                self._read_loop()
            except Exception as exc:
                log.warning("Subscriber error: %s — reconnecting in 5 s", exc)
                self._close()
                time.sleep(5)

    def _connect(self):
        self._close()
        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode    = ssl.CERT_NONE
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(self.timeout)
        raw.connect((self.host, self.port))
        self._sock = ctx.wrap_socket(raw, server_hostname=self.host)
        # PING_INTERVAL as recv timeout: _read_loop wakes up every 5 minutes
        # to send server.ping when no notifications arrive.
        self._sock.settimeout(PING_INTERVAL)
        self._buf = b""
        log.info("Subscriber connected to %s:%s", self.host, self.port)

        self._send("server.version",               [f"{CLIENT_NAME} {CLIENT_VERSION}", [PROTOCOL_MIN, PROTOCOL_MAX]])
        self._send("blockchain.headers.subscribe",  [])

        with self._sub_lock:
            hashes = list(self._subscribed)
        for sh in hashes:
            self._send("blockchain.scripthash.subscribe", [sh])

    def _send(self, method, params):
        with self._send_lock:
            self._req_id += 1
            payload = json.dumps({"id": self._req_id, "method": method, "params": params}) + "\n"
            if self._sock:
                self._sock.sendall(payload.encode())

    def _read_loop(self):
        while True:
            try:
                chunk = self._sock.recv(4096)
            except socket.timeout:
                # No data for PING_INTERVAL seconds — keep the ElectrumX
                # session alive with an application-level ping.
                log.debug("Subscriber idle — sending server.ping")
                self._send("server.ping", [])
                # Response {"id": N, "result": null} arrives on next recv
                # and is silently dropped by _dispatch (no "method" field).
                continue

            if not chunk:
                raise ConnectionError("Server closed connection")

            self._buf += chunk
            while b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    self._dispatch(json.loads(line))
                except Exception:
                    pass

    def _dispatch(self, msg):
        method = msg.get("method")
        if not method:
            # Response to our own request (server.version / server.ping /
            # blockchain.*.subscribe) — ignore silently.
            return

        params = msg.get("params") or []

        if method == "blockchain.headers.subscribe":
            if params and isinstance(params[0], dict) and self.on_new_block:
                height = params[0].get("height")
                if height is not None:
                    try:
                        self.on_new_block(int(height))
                    except Exception:
                        pass

        elif method == "blockchain.scripthash.subscribe":
            if params and self.on_scripthash_change:
                try:
                    self.on_scripthash_change(str(params[0]))
                except Exception:
                    pass

    def _close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        self._buf = b""
