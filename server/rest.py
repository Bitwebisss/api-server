# server/rest.py
"""
REST API blueprint.

Endpoints
---------
GET  /info                     Current block height
GET  /balance/<address>        Confirmed + unconfirmed balance (satoshis)
GET  /unspent/<address>        UTXO list; optional ?amount=<min> ?confirmed=true
GET  /fee                      Fixed fee rate (satoshis)
GET  /tx/<txid>                Verbose transaction (vout includes value_sat)
GET  /history/<address>        List of {tx_hash, height}; height==0 = mempool
POST /broadcast                Broadcast raw transaction hex
"""

import logging
import gevent
import gevent.pool
import re
import threading
import time

from flask import Blueprint, jsonify, request
from flask_cors import cross_origin
from flask_socketio import join_room

from server.electrum import ElectrumPool, ElectrumSubscriber
from server.address  import address_to_scripthash, address_to_scriptpubkey
from server           import utils, socketio
import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ElectrumX connection pool (POOL_SIZE parallel connections for HTTP requests)
# ---------------------------------------------------------------------------
_pool = ElectrumPool(
    host       = config.ELECTRUM_HOST,
    port       = config.ELECTRUM_PORT,
    timeout    = config.ELECTRUM_TIMEOUT,
    verify_ssl = config.ELECTRUM_VERIFY_SSL,
    size       = config.ELECTRUM_POOL_SIZE,
)

# ---------------------------------------------------------------------------
# ElectrumX subscriber (dedicated connection for push notifications)
# Starts once when rest.init() is called.
# ---------------------------------------------------------------------------
_subscriber = ElectrumSubscriber(
    host       = config.ELECTRUM_HOST,
    port       = config.ELECTRUM_PORT,
    timeout    = config.ELECTRUM_TIMEOUT,
    verify_ssl = config.ELECTRUM_VERIFY_SSL,
)

bp = Blueprint("api", __name__)

# ---------------------------------------------------------------------------
# In-memory coinbase cache  {txid: bool}
# Coinbase status is immutable once mined — cache forever, never invalidate.
# Bounded to MAX_COINBASE_CACHE_SIZE entries; when full, oldest half is evicted.
# ---------------------------------------------------------------------------
_coinbase_cache: dict[str, bool] = {}
_coinbase_lock  = threading.Lock()
MAX_COINBASE_CACHE_SIZE = 100_000   # ~20 MB max

# ---------------------------------------------------------------------------
# Tip height cache  — /info subscribes once, then caches the result for 5 s
# so that rapid polling doesn't accumulate server-push notifications in the
# ElectrumX read buffer.
# ---------------------------------------------------------------------------
_tip_cache: dict = {}          # {"height": int, "expires": float}
_TIP_TTL = 5.0                 # seconds

def _get_tip_height() -> int:
    """
    Return current chain tip height.

    Reads from _tip_cache when fresh (populated by _on_new_block or a prior
    call).  Falls back to blockchain.headers.subscribe on the pool exactly once
    when the cache is cold (first request before any block notification).

    Thread-safe: uses _tip_lock for the cache-miss path.
    """
    now = time.monotonic()
    # Fast path — no lock needed for a read when cache is hot
    if _tip_cache.get("height") is not None and now < _tip_cache.get("expires", 0):
        return _tip_cache["height"]

    with _tip_lock:
        # Re-check inside the lock (another thread may have just populated it)
        now = time.monotonic()
        if _tip_cache.get("height") is not None and now < _tip_cache.get("expires", 0):
            return _tip_cache["height"]

        # blockchain.headers.subscribe returns the current tip on every call.
        # Pool connections correctly discard any subsequent server-push
        # notifications in ElectrumClient._rpc, so calling it here is safe.
        tip = _pool.call("blockchain.headers.subscribe")
        _tip_cache["height"]  = tip["height"]
        _tip_cache["expires"] = time.monotonic() + _TIP_TTL
        return _tip_cache["height"]

# ---------------------------------------------------------------------------
# Coinbase detection helper
# ---------------------------------------------------------------------------

def _is_coinbase(txid: str) -> bool:
    """
    Return True if txid is a coinbase transaction.
    Uses in-memory cache — fetches full TX at most once per txid.
    Double-checked locking: only one greenlet writes, others reuse result.
    """
    with _coinbase_lock:
        if txid in _coinbase_cache:
            return _coinbase_cache[txid]

    try:
        raw = _pool.call("blockchain.transaction.get", txid, True)
        vin = raw.get("vin", [])
        result = bool(vin and "coinbase" in vin[0])
    except Exception:
        return False

    with _coinbase_lock:
        if txid not in _coinbase_cache:
            if len(_coinbase_cache) >= MAX_COINBASE_CACHE_SIZE:
                evict_count = MAX_COINBASE_CACHE_SIZE // 2
                for key in list(_coinbase_cache.keys())[:evict_count]:
                    del _coinbase_cache[key]
                log.info("Coinbase cache evicted %d entries (was full)", evict_count)
            _coinbase_cache[txid] = result
    return result

# ---------------------------------------------------------------------------
# Verbose TX cache  — confirmed TXs are immutable, cache forever.
# Bounded to MAX_TX_CACHE_SIZE; oldest half evicted when full.
# ---------------------------------------------------------------------------
_tx_cache: dict[str, dict] = {}
_tx_cache_lock = threading.Lock()
MAX_TX_CACHE_SIZE = 50_000   # ~< 100 MB depending on tx size

# ---------------------------------------------------------------------------
# History result cache  {scripthash: raw_history_list}
#
# blockchain.scripthash.get_history is the most-called ElectrumX method and
# the result only changes when a new TX arrives or confirms.  We cache it per
# scripthash and invalidate it in _on_scripthash_change().
#
# This eliminates the dominant source of pool exhaustion under load: without
# the cache every /history request hammers the pool even when nothing changed.
# ---------------------------------------------------------------------------
_history_cache: dict[str, list] = {}
_history_cache_lock = threading.Lock()
MAX_HISTORY_CACHE_SIZE = 50_000  # scripthashes; each entry is a small list


def _get_tx_verbose(txid: str) -> dict:
    """Fetch verbose TX from ElectrumX, serving from cache when available.

    Only confirmed transactions (those with a blockhash/blocktime) are cached.
    Unconfirmed (mempool) transactions are never stored: they can gain a
    blocktime once mined, and caching them would cause stale timestamp=None
    forever even after confirmation.
    """
    with _tx_cache_lock:
        if txid in _tx_cache:
            return _tx_cache[txid]

    raw = _pool.call("blockchain.transaction.get", txid, True)

    # Кешируем только подтверждённые TX — у них есть blockhash или blocktime.
    # Мемпуловые TX не кешируем: после майнинга они обретают blocktime,
    # и устаревший кеш вернул бы timestamp=None навсегда.
    is_confirmed = bool(raw.get("blockhash") or raw.get("blocktime") or raw.get("confirmations", 0) > 0)
    if is_confirmed:
        with _tx_cache_lock:
            if txid not in _tx_cache:
                if len(_tx_cache) >= MAX_TX_CACHE_SIZE:
                    for key in list(_tx_cache.keys())[:MAX_TX_CACHE_SIZE // 2]:
                        del _tx_cache[key]
                    log.info("TX cache evicted %d entries", MAX_TX_CACHE_SIZE // 2)
                _tx_cache[txid] = raw
    return raw


def _vout_address(vout_entry: dict) -> str | None:
    """Extract address string from a verbose vout entry (handles old/new ElectrumX)."""
    spk = vout_entry.get("scriptPubKey", {})
    addr = spk.get("address")
    if addr:
        return str(addr)
    addrs = spk.get("addresses")
    if addrs and isinstance(addrs, list) and addrs:
        return str(addrs[0])
    return None


def _vout_sats(vout_entry: dict) -> int:
    """Return vout value in satoshis."""
    return int(round(vout_entry.get("value", 0) * 1e8))


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

# Exactly 64 lowercase or uppercase hex chars
_RE_TXID = re.compile(r'^[0-9a-fA-F]{64}$')

# bech32 addresses (web1q...) are ~62 chars; base58 are 25-34 chars.
# Allow alphanumeric only, 25-90 chars — any deeper check is done by
# address_to_scripthash which will raise ValueError for garbage.
_RE_ADDRESS = re.compile(r'^[a-zA-Z0-9]{25,90}$')

# Raw transaction hex: even-length hex string, max 100 KB (200 000 hex chars)
_RE_HEX     = re.compile(r'^[0-9a-fA-F]+$')
_BROADCAST_MAX_BYTES = 100_000   # 100 KB raw tx hex ceiling

def _check_address(address: str):
    """Raise ValueError if address looks obviously wrong before hitting ElectrumX."""
    if not address or not _RE_ADDRESS.match(address):
        raise ValueError(
            "Invalid address format — expected alphanumeric, 25-90 characters"
        )

def _check_txid(txid: str):
    """Raise ValueError if txid is not exactly 64 hex chars."""
    if not txid or not _RE_TXID.match(txid):
        raise ValueError("Invalid txid — expected 64 hex characters")

# ---------------------------------------------------------------------------
# /info
# ---------------------------------------------------------------------------

_tip_lock = threading.Lock()

@bp.route("/info", methods=["GET"])
@cross_origin()
def get_info():
    try:
        height = _get_tip_height()
        return jsonify(utils.ok({"blocks": height}))
    except Exception as exc:
        log.exception("GET /info")
        return jsonify(utils.err(500, str(exc))), 500


# ---------------------------------------------------------------------------
# /balance/<address>
# ---------------------------------------------------------------------------

@bp.route("/balance/<string:address>", methods=["GET"])
@cross_origin()
def get_balance(address: str):
    try:
        _check_address(address)
        scripthash = address_to_scripthash(address)
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    try:
        data  = _pool.call("blockchain.scripthash.get_balance", scripthash)
        total = data["confirmed"] + data["unconfirmed"]
        return jsonify(utils.ok({
            "balance":     total,
            "confirmed":   data["confirmed"],
            "unconfirmed": data["unconfirmed"],
        }))
    except Exception as exc:
        log.exception("GET /balance/%s", address)
        return jsonify(utils.err(500, str(exc))), 500


# ---------------------------------------------------------------------------
# /unspent/<address>
# ---------------------------------------------------------------------------

@bp.route("/unspent/<string:address>", methods=["GET"])
@cross_origin()
def get_unspent(address: str):
    try:
        _check_address(address)
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    # Validate query params explicitly — ignore anything unexpected
    try:
        raw_amount = request.args.get("amount", "0")
        if not re.match(r'^\d{1,15}$', raw_amount):
            return jsonify(utils.err(400, "amount must be a non-negative integer")), 400
        min_value = int(raw_amount)
    except (ValueError, TypeError):
        min_value = 0

    confirmed_only = request.args.get("confirmed", "false").lower() == "true"

    try:
        scripthash = address_to_scripthash(address)
        script_hex = address_to_scriptpubkey(address).hex()
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    try:
        utxos = _pool.call("blockchain.scripthash.listunspent", scripthash)
    except Exception as exc:
        log.exception("GET /unspent/%s", address)
        return jsonify(utils.err(500, str(exc))), 500

    # Сначала фильтруем — не дёргаем coinbase для отброшенных UTXO
    filtered = [
        u for u in utxos
        if not (confirmed_only and u["height"] == 0)
        and not (min_value > 0 and u["value"] < min_value)
    ]

    def _enrich(u):
        return {
            "txid":     u["tx_hash"],
            "index":    u["tx_pos"],
            "value":    u["value"],
            "height":   u["height"],
            "script":   script_hex,
            "coinbase": _is_coinbase(u["tx_hash"]),
        }

    # Все coinbase-запросы летят параллельно (кэш — большинство хитов)
    # size ограничен 20 — не создаём лишних гринлетов при малом числе UTXO
    if filtered:
        gpool = gevent.pool.Pool(min(len(filtered), 20))
        result = list(gpool.imap(_enrich, filtered))
    else:
        result = []
    return jsonify(utils.ok(result))


# ---------------------------------------------------------------------------
# /fee
# ---------------------------------------------------------------------------

@bp.route("/fee", methods=["GET"])
@cross_origin()
def get_fee():
    return jsonify(utils.ok({"feerate": config.FIXED_FEE_SATOSHIS}))


# ---------------------------------------------------------------------------
# /tx/<txid>
# ---------------------------------------------------------------------------

@bp.route("/tx/<string:txid>", methods=["GET"])
@cross_origin()
def get_tx(txid: str):
    """
    Return verbose transaction from ElectrumX.

    vout items get an extra field  value_sat (int satoshis) in addition to
    the native float  value  field, so clients avoid float math.
    """
    try:
        _check_txid(txid)
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    try:
        raw = _pool.call("blockchain.transaction.get", txid, True)
    except RuntimeError as exc:
        return jsonify(utils.err(400, str(exc))), 400
    except Exception as exc:
        log.exception("GET /tx/%s", txid)
        return jsonify(utils.err(500, str(exc))), 500

    for out in raw.get("vout", []):
        out["value_sat"] = int(round(out.get("value", 0) * 1e8))

    return jsonify(utils.ok(raw))


# ---------------------------------------------------------------------------
# /history/<address>
# ---------------------------------------------------------------------------

@bp.route("/history/<string:address>", methods=["GET"])
@cross_origin()
def get_history(address: str):
    """
    Return last N transactions for *address*, annotated with direction and amount.

    Algorithm (same as Electrum wallet — works for Legacy, SegWit, Taproot,
    multi-send, consolidation, send-to-self):

      1. blockchain.scripthash.get_history  → list of {tx_hash, height}
      2. blockchain.transaction.get(txid, verbose=True) for each recent TX
         → full vin (with prevout txid+index) and vout (with scriptPubKey.address)
      3. For every non-coinbase vin, fetch the *previous* TX to read the address
         and value of the output being spent. All fetches run in parallel.
      4. Direction:
           mine_in  = Σ value of inputs  whose prevout address == our address
           mine_out = Σ value of outputs whose address         == our address

           mine_in > 0, mine_out >= mine_in          → 'self' (consolidation or
                                                        batched incoming+change)
           mine_in > 0, has external out,
                        mine_out < mine_in            → 'out'  (net = mine_in − mine_out)
           mine_in == 0, mine_out > 0                → 'in'
           otherwise                                 → 'unknown'

    Confirmed TXs are cached in _tx_cache; repeated calls are cheap.

    Query params:
      limit  — max entries returned (default 10, max 50)

    Response: {"result": [...], "error": null}
    Item fields: txid, height, timestamp, direction, amount, mine_in, mine_out
    """
    try:
        limit = min(int(request.args.get("limit", 10)), 50)
    except (ValueError, TypeError):
        limit = 10

    try:
        _check_address(address)
        scripthash = address_to_scripthash(address)
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    try:
        # Serve from cache when available; invalidated by _on_scripthash_change().
        with _history_cache_lock:
            _raw = _history_cache.get(scripthash)

        if _raw is None:
            _raw = _pool.call("blockchain.scripthash.get_history", scripthash)
            with _history_cache_lock:
                if scripthash not in _history_cache:
                    if len(_history_cache) >= MAX_HISTORY_CACHE_SIZE:
                        evict = MAX_HISTORY_CACHE_SIZE // 2
                        for k in list(_history_cache.keys())[:evict]:
                            del _history_cache[k]
                        log.info("History cache evicted %d entries", evict)
                    _history_cache[scripthash] = _raw

        history = []
        seen: set[str] = set()
        for _h in _raw:
            _height = _h["height"]
            if _height < -1:
                # height < -1 is not defined by the ElectrumX protocol — skip.
                continue
            if _height == -1:
                # height=-1: mempool TX whose inputs are also unconfirmed.
                # Normalise to 0 so the frontend shows a "pending" badge.
                _h = dict(_h, height=0)
            # Deduplicate by txid.  During a reorg ElectrumX can return the
            # same txid twice: once confirmed (height=N) and again as mempool
            # (height=0).  Keep the first occurrence (confirmed takes priority).
            if _h["tx_hash"] not in seen:
                seen.add(_h["tx_hash"])
                history.append(_h)

        # Reorg cache invalidation: if a TX we previously cached as confirmed now
        # appears at height=0 (reorg put it back in the mempool), its cached
        # verbose object is stale — it still carries the old blockhash/blocktime.
        # Evict it so _get_tx_verbose fetches a fresh copy from the node.
        with _tx_cache_lock:
            for _h in history:
                if _h["height"] == 0 and _h["tx_hash"] in _tx_cache:
                    log.debug("Evicting stale tx cache after reorg: %s", _h["tx_hash"][:16])
                    del _tx_cache[_h["tx_hash"]]

    except Exception as exc:
        log.exception("GET /history/%s — history fetch", address)
        return jsonify(utils.err(500, str(exc))), 500

    recent = history[-limit:][::-1]   # most-recent N, newest first
    if not recent:
        return jsonify(utils.ok([]))

    try:
        # Step 1 — fetch main TXs in parallel
        def _fetch_main(item):
            try:
                return (item, _get_tx_verbose(item["tx_hash"]))
            except Exception as e:
                log.warning("history: failed to fetch tx %s: %s", item["tx_hash"][:12], e)
                return (item, None)

        main_pool = gevent.pool.Pool(min(len(recent), 8))
        main_txs  = list(main_pool.imap(_fetch_main, recent))

        # Step 2 — collect unique prevout txids we need to resolve inputs
        prevout_needed: set[str] = set()
        for _item, tx in main_txs:
            if tx is None:
                continue
            for vin in tx.get("vin", []):
                if "coinbase" not in vin and "txid" in vin:
                    prevout_needed.add(vin["txid"])

        # Step 3 — fetch prevout TXs in parallel (mostly cache hits after first call)
        def _fetch_prevout(ptxid):
            try:
                return (ptxid, _get_tx_verbose(ptxid))
            except Exception as e:
                log.warning("history: failed to fetch prevout %s: %s", ptxid[:12], e)
                return (ptxid, None)

        if prevout_needed:
            p_pool      = gevent.pool.Pool(min(len(prevout_needed), 16))
            prevout_map = dict(p_pool.imap(_fetch_prevout, prevout_needed))
        else:
            prevout_map = {}

        # Step 4 — annotate each TX
        results = []
        for item, tx in main_txs:
            if tx is None:
                results.append({
                    "txid": item["tx_hash"], "height": item["height"],
                    "timestamp": None, "direction": "unknown",
                    "amount": None, "mine_in": 0, "mine_out": 0,
                })
                continue

            timestamp = tx.get("blocktime") or tx.get("time") or None
            mine_in   = 0
            mine_out  = 0

            for vin in tx.get("vin", []):
                if "coinbase" in vin:
                    continue
                ptxid = vin.get("txid")
                pvout = vin.get("vout")
                if ptxid is None or pvout is None:
                    continue
                ptx = prevout_map.get(ptxid)
                if ptx is None:
                    continue
                pvouts = ptx.get("vout", [])
                if pvout < 0 or pvout >= len(pvouts):
                    continue
                if _vout_address(pvouts[pvout]) == address:
                    mine_in += _vout_sats(pvouts[pvout])

            has_external_out = False
            for vout in tx.get("vout", []):
                addr = _vout_address(vout)
                if addr is None:
                    continue          # OP_RETURN / undecodable — skip
                if addr == address:
                    mine_out += _vout_sats(vout)
                else:
                    has_external_out = True

            if mine_in > 0:
                net = mine_in - mine_out
                if not has_external_out or mine_out >= mine_in:
                    # All outputs return to us, or we received at least as much
                    # as we spent (batched TX where we are also a recipient).
                    # Both cases match Electrum wallet's "self" convention.
                    direction = "self"
                    amount    = mine_out
                else:
                    direction = "out"
                    # net < 0 is impossible in a valid TX but guard to prevent
                    # a negative amount reaching the response.
                    amount    = max(net, 0)
            elif mine_out > 0:
                direction = "in"
                amount    = mine_out
            else:
                direction = "unknown"
                amount    = 0

            results.append({
                "txid":      item["tx_hash"],
                "height":    item["height"],
                "timestamp": timestamp,
                "direction": direction,
                "amount":    amount,
                "mine_in":   mine_in,
                "mine_out":  mine_out,
            })

        return jsonify(utils.ok(results))

    except Exception as exc:
        log.exception("GET /history/%s", address)
        return jsonify(utils.err(500, str(exc))), 500


# ---------------------------------------------------------------------------
# /rawtx/<txid>
# ---------------------------------------------------------------------------

@bp.route("/rawtx/<string:txid>", methods=["GET"])
@cross_origin()
def get_raw_tx(txid: str):
    """
    Return raw transaction hex string.
    Required by the web wallet for signing legacy P2PKH inputs (nonWitnessUtxo).
    Response: { "result": "<hex>", "error": null }
    """
    try:
        _check_txid(txid)
    except ValueError as exc:
        return jsonify(utils.err(400, str(exc))), 400

    try:
        raw_hex = _pool.call("blockchain.transaction.get", txid, False)
        return jsonify(utils.ok(raw_hex))
    except RuntimeError as exc:
        return jsonify(utils.err(400, str(exc))), 400
    except Exception as exc:
        log.exception("GET /rawtx/%s", txid)
        return jsonify(utils.err(500, str(exc))), 500


# ---------------------------------------------------------------------------
# /broadcast
# ---------------------------------------------------------------------------

@bp.route("/broadcast", methods=["POST"])
@cross_origin()
def broadcast():
    # Reject oversized bodies before reading
    if request.content_length and request.content_length > _BROADCAST_MAX_BYTES:
        return jsonify(utils.err(400, "Request body too large (max 100 KB)")), 400

    raw_tx = request.values.get("raw") or request.get_data(as_text=True).strip()

    if not raw_tx:
        return jsonify(utils.err(400, "Missing raw transaction hex")), 400

    # Length guard (in case content_length header was absent or spoofed)
    if len(raw_tx) > _BROADCAST_MAX_BYTES:
        return jsonify(utils.err(400, "Transaction hex too large (max 100 KB)")), 400

    # Must be pure hex and even-length (full bytes)
    if len(raw_tx) % 2 != 0 or not _RE_HEX.match(raw_tx):
        return jsonify(utils.err(400, "raw must be a valid hex string")), 400

    try:
        txid = _pool.call("blockchain.transaction.broadcast", raw_tx)
        return jsonify(utils.ok(txid))
    except RuntimeError as exc:
        msg = str(exc)
        log.warning("POST /broadcast rejected: %s", msg)
        return jsonify(utils.err(400, msg)), 400
    except Exception as exc:
        log.exception("POST /broadcast")
        return jsonify(utils.err(500, str(exc))), 500


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def init(app):
    app.register_blueprint(bp, url_prefix="/")
    _start_subscriber()


# ---------------------------------------------------------------------------
# WebSocket — socket.io events (default namespace)
# ---------------------------------------------------------------------------

# per-sid mapping: sid -> currently subscribed scripthash
_sid_rooms: dict = {}

# scripthash -> script hex (used when pushing UTXOs to clients)
_scripthash_scripts: dict = {}

# scripthash -> number of currently subscribed sids  (O(1) membership check)
_scripthash_refcount: dict = {}

_sid_lock = threading.Lock()


@socketio.on("connect")
def _ws_connect():
    log.debug("WS connected: %s", request.sid)


@socketio.on("disconnect")
def _ws_disconnect():
    with _sid_lock:
        old_sh = _sid_rooms.pop(request.sid, None)
        if old_sh:
            count = _scripthash_refcount.get(old_sh, 1) - 1
            if count <= 0:
                # No subscribers left — free associated state
                _scripthash_refcount.pop(old_sh, None)
                _scripthash_scripts.pop(old_sh, None)
            else:
                _scripthash_refcount[old_sh] = count


@socketio.on("subscribe")
def _ws_subscribe(data):
    if not isinstance(data, dict):
        return

    address = str(data.get("address", "")).strip()
    try:
        _check_address(address)
        scripthash = address_to_scripthash(address)
        script_hex = address_to_scriptpubkey(address).hex()
    except ValueError as exc:
        socketio.emit("error", {"message": str(exc)}, to=request.sid)
        return

    with _sid_lock:
        old_room = _sid_rooms.get(request.sid)
        _sid_rooms[request.sid] = scripthash
        _scripthash_scripts[scripthash] = script_hex

        # Increment refcount for the new scripthash
        _scripthash_refcount[scripthash] = _scripthash_refcount.get(scripthash, 0) + 1

        # Decrement refcount for the previous scripthash (if the client switched address)
        if old_room and old_room != scripthash:
            count = _scripthash_refcount.get(old_room, 1) - 1
            if count <= 0:
                _scripthash_refcount.pop(old_room, None)
                _scripthash_scripts.pop(old_room, None)
            else:
                _scripthash_refcount[old_room] = count

    if old_room and old_room != scripthash:
        from flask_socketio import leave_room as _leave
        _leave(old_room)

    join_room(scripthash)
    _subscriber.subscribe_scripthash(scripthash)
    socketio.emit("subscribed", {"address": address}, to=request.sid)

    # Capture sid before spawn — request context is gone inside the greenlet
    sid = request.sid

    def _send_initial():
        try:
            bal       = _pool.call("blockchain.scripthash.get_balance", scripthash)
            raw_utxos = _pool.call("blockchain.scripthash.listunspent", scripthash)
            height    = _get_tip_height()

            sx = _scripthash_scripts.get(scripthash, "")

            def _enrich_initial(u):
                return {
                    "txid":     u["tx_hash"],
                    "index":    u["tx_pos"],
                    "value":    u["value"],
                    "height":   u["height"],
                    "coinbase": _is_coinbase(u["tx_hash"]),
                    "script":   sx,
                }

            if raw_utxos:
                gpool = gevent.pool.Pool(min(len(raw_utxos), 20))
                enriched = list(gpool.imap(_enrich_initial, raw_utxos))
            else:
                enriched = []

            incoming_mempool = sum(u["value"] for u in raw_utxos if u["height"] == 0)
            pending_out      = max(0, incoming_mempool - bal["unconfirmed"])

            payload = {
                "balance":     bal["confirmed"] + bal["unconfirmed"],
                "confirmed":   bal["confirmed"],
                "unconfirmed": bal["unconfirmed"],
                "pending_out": pending_out,
                "utxos":       enriched,
                "height":      height,
            }
            # Send only to this specific client, not the whole room
            socketio.emit("balance_changed", payload, to=sid)
        except Exception as e:
            log.warning("Initial push failed for %s: %s", scripthash[:12], e)

    gevent.spawn(_send_initial)


# ---------------------------------------------------------------------------
# Subscriber callbacks → emit to connected clients
# ---------------------------------------------------------------------------

def _on_new_block(height: int) -> None:
    """Called by ElectrumSubscriber when a new block arrives."""
    _tip_cache["height"]  = height
    _tip_cache["expires"] = time.monotonic() + _TIP_TTL
    log.info("New block: height=%d — pushing to all WS clients", height)
    socketio.emit("block", {"height": height})


def _on_scripthash_change(scripthash: str) -> None:
    """Called when ElectrumX reports a status change on a watched scripthash."""
    log.debug("Balance changed: scripthash=%s…", scripthash[:12])

    # Invalidate the history cache for this scripthash so the next /history
    # request fetches fresh data from ElectrumX instead of the stale list.
    with _history_cache_lock:
        _history_cache.pop(scripthash, None)

    def _fetch_and_push():
        # O(1) check — skip fetch if no clients are watching this scripthash
        with _sid_lock:
            anyone = _scripthash_refcount.get(scripthash, 0) > 0
        if not anyone:
            return

        try:
            bal       = _pool.call("blockchain.scripthash.get_balance", scripthash)
            raw_utxos = _pool.call("blockchain.scripthash.listunspent", scripthash)
            # _on_new_block() always fires before _on_scripthash_change() for the
            # same block, so the cache is already fresh here.  No need to hit the
            # pool for the tip — just read what the subscriber already wrote.
            height    = _tip_cache.get("height", 0)

            sx = _scripthash_scripts.get(scripthash, "")

            def _enrich_push(u):
                return {
                    "txid":     u["tx_hash"],
                    "index":    u["tx_pos"],
                    "value":    u["value"],
                    "height":   u["height"],
                    "coinbase": _is_coinbase(u["tx_hash"]),
                    "script":   sx,
                }

            if raw_utxos:
                gpool = gevent.pool.Pool(min(len(raw_utxos), 20))
                enriched = list(gpool.imap(_enrich_push, raw_utxos))
            else:
                enriched = []

            incoming_mempool = sum(u["value"] for u in raw_utxos if u["height"] == 0)
            pending_out      = max(0, incoming_mempool - bal["unconfirmed"])

            payload = {
                "balance":     bal["confirmed"] + bal["unconfirmed"],
                "confirmed":   bal["confirmed"],
                "unconfirmed": bal["unconfirmed"],
                "pending_out": pending_out,
                "utxos":       enriched,
                "height":      height,
            }
            socketio.emit("balance_changed", payload, to=scripthash)

        except Exception as e:
            log.warning("Failed to push balance for scripthash %s: %s",
                        scripthash[:12], e)

    gevent.spawn(_fetch_and_push)


def _start_subscriber() -> None:
    _subscriber.on_new_block         = _on_new_block
    _subscriber.on_scripthash_change = _on_scripthash_change
    _subscriber.start()
    log.info("ElectrumX subscriber started")
