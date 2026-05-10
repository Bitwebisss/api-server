import hashlib

from bech32 import bech32_decode, convertbits
import base58 as _b58

_BECH32M_CONST = 0x2BC830A3

def _bech32_polymod(values):
    """Internal polymod for bech32/bech32m."""
    gen = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = (chk >> 25)
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= gen[i] if ((b >> i) & 1) else 0
    return chk

def _bech32_hrp_expand(hrp: str) -> list:
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]

def bech32m_decode(bech: str):
    """Decode a bech32m string. Returns (hrp, data) or (None, None)."""
    if not bech.islower() and not bech.isupper():
        bech = bech.lower()
    pos = bech.rfind('1')
    if pos < 1 or pos + 7 > len(bech):
        return None, None
    hrp = bech[:pos]
    data = []
    for ch in bech[pos+1:]:
        d = "qpzry9x8gf2tvdw0s3jn54khce6mua7l".find(ch)
        if d == -1:
            return None, None
        data.append(d)
    if _bech32_polymod(_bech32_hrp_expand(hrp) + data) != _BECH32M_CONST:
        return None, None
    return hrp, data[:-6]   # strip checksum

_BECH32_HRPS = frozenset(["web"])

# Version byte → script type
_P2PKH_VERSIONS = frozenset([0x21, 0x42])
_P2SH_VERSIONS  = frozenset([0x1E, 0x80])


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _is_bech32(addr: str) -> bool:
    lower = addr.lower()
    for hrp in _BECH32_HRPS:
        if lower.startswith(hrp + "1"):
            return True
    return False


def _bech32_to_scriptpubkey(addr: str) -> bytes:
    lower = addr.lower()

    # Try witness v0 (bech32) first, then witness v1+ (bech32m)
    hrp, data = bech32_decode(lower)
    is_bech32m = False
    if hrp is None or data is None:
        hrp, data = bech32m_decode(lower)
        is_bech32m = True

    if hrp is None or data is None:
        raise ValueError(
            f"Invalid bech32/bech32m address: {addr!r}  "
            f"(expected format: web1q... or web1p...)"
        )
    if hrp not in _BECH32_HRPS:
        raise ValueError(f"Unknown HRP {hrp!r}: {addr!r}")
    if not data:
        raise ValueError(f"Empty bech32 data: {addr!r}")

    witness_version = data[0]
    if witness_version == 0:
        if is_bech32m:
            raise ValueError(f"Witness v0 must use bech32 encoding, not bech32m: {addr!r}")
        program = convertbits(data[1:], 5, 8, False)
        if program is None:
            raise ValueError(f"Cannot decode witness program: {addr!r}")
        prog = bytes(program)
        if len(prog) == 20:
            return b"\x00\x14" + prog        # P2WPKH
        if len(prog) == 32:
            return b"\x00\x20" + prog        # P2WSH
        raise ValueError(f"Witness v0 program must be 20 or 32 bytes, got {len(prog)}: {addr!r}")

    # Witness v1 (Taproot / P2TR) — must be bech32m
    if witness_version == 1:
        if not is_bech32m:
            raise ValueError(f"Witness v1 must use bech32m encoding: {addr!r}")
        program = convertbits(data[1:], 5, 8, False)
        if program is None:
            raise ValueError(f"Cannot decode witness program: {addr!r}")
        prog = bytes(program)
        if len(prog) != 32:
            raise ValueError(f"Witness v1 (Taproot) program must be 32 bytes, got {len(prog)}: {addr!r}")
        return b"\x51\x20" + prog            # OP_1 <32-byte x-only pubkey> = P2TR

    raise ValueError(f"Unsupported witness version {witness_version}: {addr!r}")


def _base58_to_scriptpubkey(addr: str) -> bytes:
    try:
        raw = _b58.b58decode_check(addr)
    except Exception as exc:
        raise ValueError(f"Invalid base58check address {addr!r}: {exc}") from exc

    if len(raw) != 21:
        raise ValueError(f"Expected 21 decoded bytes, got {len(raw)}: {addr!r}")

    version, hash160 = raw[0], raw[1:]

    if version in _P2PKH_VERSIONS:
        return b"\x76\xa9\x14" + hash160 + b"\x88\xac"    # P2PKH
    if version in _P2SH_VERSIONS:
        return b"\xa9\x14" + hash160 + b"\x87"            # P2SH
    raise ValueError(f"Unknown version byte 0x{version:02X}: {addr!r}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def address_to_scriptpubkey(addr: str) -> bytes:
    if _is_bech32(addr):
        return _bech32_to_scriptpubkey(addr)
    return _base58_to_scriptpubkey(addr)


def address_to_scripthash(addr: str) -> str:
    script = address_to_scriptpubkey(addr)
    return _sha256(script)[::-1].hex()
