"""Drain buffered JSON-RPC stdout independently from request/reply waiting."""
import json
import queue
import threading
import time


class RpcReplies:
    def __init__(self, stream):
        self.pending = queue.Queue()
        threading.Thread(target=self._read, args=(stream,), daemon=True).start()

    def _read(self, stream):
        try:
            for line in stream:
                self.pending.put(json.loads(line))
        except Exception as error:
            self.pending.put(error)
        finally:
            self.pending.put(None)

    def response(self, request_id, timeout, observe):
        deadline = time.monotonic() + timeout
        while True:
            try:
                response = self.pending.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty as error:
                raise TimeoutError(f"JSON-RPC request {request_id}") from error
            if response is None:
                raise RuntimeError("app-server stdout closed")
            if isinstance(response, Exception):
                raise response
            observe(response)
            if response.get("id") == request_id:
                if "error" in response:
                    raise RuntimeError(json.dumps(response))
                return response.get("result")
