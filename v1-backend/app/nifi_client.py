import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


class NiFiClient:
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: bool = False,
        timeout: int = 15,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username or ""
        self.password = password or ""
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self._token: Optional[str] = None

    def _ssl_context(self):
        if self.verify_ssl:
            return None
        return ssl._create_unverified_context()

    def _request(
        self,
        method: str,
        path: str,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url=url, data=data, method=method)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_context()) as resp:
                body = resp.read().decode("utf-8")
                if not body:
                    return None
                ct = resp.headers.get("Content-Type", "")
                if "application/json" in ct:
                    return json.loads(body)
                return body
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else str(e)
            raise RuntimeError(f"NiFi HTTP {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"NiFi URL error: {e}") from e

    def login(self) -> str:
        if self._token:
            return self._token
        if not self.username or not self.password:
            raise RuntimeError("NiFi username/password not configured")
        payload = urllib.parse.urlencode({"username": self.username, "password": self.password}).encode("utf-8")
        token = self._request(
            "POST",
            "/nifi-api/access/token",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if not token:
            raise RuntimeError("NiFi login failed: empty token")
        self._token = token if isinstance(token, str) else str(token)
        return self._token

    def _auth_headers(self) -> Dict[str, str]:
        token = self.login()
        return {"Authorization": f"Bearer {token}"}

    def get_process_group_status(self, process_group_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/nifi-api/flow/process-groups/{process_group_id}/status", headers=self._auth_headers())

    def run_process_group(self, process_group_id: str) -> Any:
        body = json.dumps(
            {
                "id": process_group_id,
                "state": "RUNNING",
                "disconnectedNodeAcknowledged": False,
            }
        ).encode("utf-8")
        return self._request(
            "PUT",
            f"/nifi-api/flow/process-groups/{process_group_id}",
            data=body,
            headers={**self._auth_headers(), "Content-Type": "application/json"},
        )

    def get_active_thread_count(self, process_group_id: str) -> int:
        status = self.get_process_group_status(process_group_id)
        aggregate = status.get("processGroupStatus", {}).get("aggregateSnapshot", {})
        return int(aggregate.get("activeThreadCount", 0) or 0)
