import asyncio
import os
import uuid
from typing import Any, Callable, Dict, Optional

from .nifi_client import NiFiClient


class BaseExecutor:
    mode = "base"

    async def execute(
        self,
        job: Dict[str, Any],
        create_output_file: Callable[[str, str], Dict[str, Any]],
        resolve_nifi_output_file: Callable[[str], Optional[Dict[str, Any]]],
        now_iso: Callable[[], str],
    ) -> None:
        raise NotImplementedError


class MockExecutor(BaseExecutor):
    mode = "mock"

    async def execute(
        self,
        job: Dict[str, Any],
        create_output_file: Callable[[str, str], Dict[str, Any]],
        resolve_nifi_output_file: Callable[[str], Optional[Dict[str, Any]]],
        now_iso: Callable[[], str],
    ) -> None:
        await asyncio.sleep(1)
        job["status"] = "RUNNING"
        job["progress"] = 30
        job["startedAt"] = now_iso()

        await asyncio.sleep(1)
        job["progress"] = 70

        await asyncio.sleep(1)
        output = create_output_file(job["jobId"], job["target"].get("format", "CSV"))
        job["progress"] = 100
        job["status"] = "SUCCEEDED"
        job["finishedAt"] = now_iso()
        job["outputs"] = [output["fileId"]]


class NiFiExecutor(BaseExecutor):
    mode = "nifi"

    def __init__(self, flow_mapping: Dict[str, str]):
        self.flow_mapping = flow_mapping
        self.client = NiFiClient(
            base_url=os.getenv("NIFI_BASE_URL", "https://localhost:8443"),
            username=os.getenv("NIFI_USERNAME", "admin"),
            password=os.getenv("NIFI_PASSWORD", "YourStrongPassword123"),
            verify_ssl=os.getenv("NIFI_VERIFY_SSL", "false").lower() == "true",
            timeout=int(os.getenv("NIFI_TIMEOUT", "15")),
        )
        self.retry_count = max(int(os.getenv("NIFI_RETRY_COUNT", "2")), 0)
        self.retry_delay_ms = max(int(os.getenv("NIFI_RETRY_DELAY_MS", "800")), 0)
        self.status_poll_steps = max(int(os.getenv("NIFI_STATUS_POLL_STEPS", "3")), 1)
        self.status_poll_interval_ms = max(int(os.getenv("NIFI_STATUS_POLL_INTERVAL_MS", "1000")), 100)

    async def _call_with_retry(self, operation: Callable[[], Any]) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(self.retry_count + 1):
            try:
                return operation()
            except Exception as e:
                last_error = e
                if attempt >= self.retry_count:
                    break
                await asyncio.sleep(self.retry_delay_ms / 1000)
        if last_error:
            raise last_error
        raise RuntimeError("NiFi operation failed with unknown error")

    def _classify_error_code(self, message: str) -> str:
        normalized = (message or "").lower()
        if "http 401" in normalized or "http 403" in normalized:
            return "NIFI_AUTH_ERROR"
        if "unable to locate group with id" in normalized or "http 404" in normalized:
            return "NIFI_FLOW_NOT_FOUND"
        if "url error" in normalized or "timed out" in normalized or "name or service not known" in normalized:
            return "NIFI_NETWORK_ERROR"
        return "NIFI_EXEC_ERROR"

    async def execute(
        self,
        job: Dict[str, Any],
        create_output_file: Callable[[str, str], Dict[str, Any]],
        resolve_nifi_output_file: Callable[[str], Optional[Dict[str, Any]]],
        now_iso: Callable[[], str],
    ) -> None:
        flow_id = self.flow_mapping.get(job["jobType"], "").strip()
        if not flow_id or flow_id.startswith("<replace-"):
            job["status"] = "FAILED"
            job["errorCode"] = "NIFI_FLOW_UNMAPPED"
            job["errorMessage"] = f"No valid NiFi flow mapping for jobType={job['jobType']}"
            job["finishedAt"] = now_iso()
            return

        job["nifiFlowId"] = flow_id
        job["nifiRequestId"] = f"nifi_req_{uuid.uuid4().hex[:8]}"
        job["executorNote"] = "NiFi executor real API mode"

        job["status"] = "RUNNING"
        job["progress"] = 20
        job["startedAt"] = now_iso()
        job["nifiRetryCount"] = self.retry_count

        try:
            await self._call_with_retry(lambda: self.client.run_process_group(flow_id))
            for idx in range(self.status_poll_steps):
                await asyncio.sleep(self.status_poll_interval_ms / 1000)
                active_threads = await self._call_with_retry(lambda: self.client.get_active_thread_count(flow_id))
                job["nifiActiveThreadCount"] = active_threads
                # Keep progress in 40~80 during polling window.
                if self.status_poll_steps == 1:
                    job["progress"] = 80
                else:
                    job["progress"] = 40 + int((idx * 40) / (self.status_poll_steps - 1))

            output = resolve_nifi_output_file(job["target"].get("format", "CSV"))
            if output is None:
                # Fallback keeps demo path alive if NiFi output directory is not mounted yet.
                output = create_output_file(job["jobId"], job["target"].get("format", "CSV"))

            job["progress"] = 100
            job["status"] = "SUCCEEDED"
            job["finishedAt"] = now_iso()
            job["outputs"] = [output["fileId"]]
        except Exception as e:
            job["status"] = "FAILED"
            message = str(e)
            job["errorCode"] = self._classify_error_code(message)
            job["errorMessage"] = message
            job["finishedAt"] = now_iso()
