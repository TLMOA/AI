"""Local host detection helpers.

Lives in its own module to avoid circular imports between
`db_connect` and `engine_factory` (both need these utilities).
"""
import os as _os
import re as _re
import socket as _socket
from typing import Optional, Set

_local_ip_cache: Optional[Set[str]] = None


def _collect_local_ips() -> Set[str]:
    """Collect all local interface IPs so we can detect 'connect to self' cases."""
    global _local_ip_cache
    if _local_ip_cache is not None:
        return _local_ip_cache
    ips = {"127.0.0.1", "::1", "localhost"}
    try:
        hostname = _socket.gethostname()
        for fam in (_socket.AF_INET, _socket.AF_INET6):
            try:
                for info in _socket.getaddrinfo(hostname, None, fam):
                    ips.add(info[4][0])
            except Exception:
                pass
    except Exception:
        pass
    try:
        for line in _os.popen("ip -o addr show 2>/dev/null").read().splitlines():
            m = _re.search(r"inet\s+(\S+)", line)
            if m:
                ips.add(m.group(1).split("/")[0])
    except Exception:
        pass
    _local_ip_cache = {ip.strip().lower() for ip in ips if ip}
    return _local_ip_cache


def _resolve_local_host(host: str) -> str:
    """If host matches a local interface IP, route to 127.0.0.1 to avoid MySQL reverse-DNS stalls."""
    if not host:
        return host
    h = host.strip()
    if h.lower() in {"localhost", "127.0.0.1", "::1"}:
        return "127.0.0.1"
    local_ips = _collect_local_ips()
    if h in local_ips or h.lower() in local_ips:
        return "127.0.0.1"
    return h
