import heapq
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Any, Optional


@dataclass(order=True)
class DelayTask:
    execute_at: float
    task_id: str = field(compare=False)
    callback: Optional[Callable] = field(compare=False, default=None)
    callback_name: Optional[str] = field(compare=False, default=None)
    args: tuple = field(compare=False, default_factory=tuple)
    kwargs: dict = field(compare=False, default_factory=dict)
    cancelled: bool = field(compare=False, default=False)
    persistent: bool = field(compare=False, default=False)


class DelayQueue:
    def __init__(self, persist_path: Optional[str] = None):
        self._heap: list[DelayTask] = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: dict[str, Callable] = {}
        self._persist_path = persist_path
        self._persist_lock = threading.Lock()

    def register_callback(self, name: str, callback: Callable[..., Any]) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("callback name must be a non-empty string")
        if not callable(callback):
            raise ValueError("callback must be callable")
        with self._lock:
            self._callbacks[name] = callback

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, wait: bool = True) -> None:
        self._running = False
        self._event.set()
        if wait and self._thread and self._thread.is_alive():
            self._thread.join()
        self._save_persistent()

    def submit(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return self._submit_internal(delay, callback=callback, callback_name=None, args=args, kwargs=kwargs, persistent=False)

    def submit_persistent(
        self,
        delay: float,
        callback_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        if not self._persist_path:
            raise RuntimeError("persist_path is not set, cannot submit persistent task")
        with self._lock:
            if callback_name not in self._callbacks:
                raise ValueError(f"callback '{callback_name}' is not registered")
        return self._submit_internal(delay, callback=None, callback_name=callback_name, args=args, kwargs=kwargs, persistent=True)

    def _submit_internal(
        self,
        delay: float,
        callback: Optional[Callable],
        callback_name: Optional[str],
        args: tuple,
        kwargs: dict,
        persistent: bool,
    ) -> str:
        if delay < 0:
            raise ValueError("delay must be non-negative")
        task_id = uuid.uuid4().hex
        task = DelayTask(
            execute_at=time.time() + delay,
            task_id=task_id,
            callback=callback,
            callback_name=callback_name,
            args=args,
            kwargs=kwargs,
            persistent=persistent,
        )
        with self._lock:
            heapq.heappush(self._heap, task)
            self._event.set()
        self._save_persistent()
        return task_id

    def cancel(self, task_id: str) -> bool:
        found = False
        with self._lock:
            for task in self._heap:
                if task.task_id == task_id:
                    task.cancelled = True
                    found = True
                    break
        if found:
            self._save_persistent()
        return found

    def _run(self) -> None:
        while self._running:
            self._event.wait()
            self._event.clear()
            while self._running:
                task_to_exec: Optional[DelayTask] = None
                with self._lock:
                    if not self._heap:
                        break
                    task = self._heap[0]
                    now = time.time()
                    if task.execute_at > now:
                        wait_time = task.execute_at - now
                        threading.Timer(wait_time, self._event.set).start()
                        break
                    heapq.heappop(self._heap)
                    task_to_exec = task
                if task_to_exec and not task_to_exec.cancelled:
                    self._execute_task(task_to_exec)
                if task_to_exec and task_to_exec.persistent:
                    self._save_persistent()

    def _execute_task(self, task: DelayTask) -> None:
        callback = task.callback
        if callback is None and task.callback_name:
            with self._lock:
                callback = self._callbacks.get(task.callback_name)
            if callback is None:
                return
        try:
            callback(*task.args, **task.kwargs)
        except Exception:
            pass

    def _save_persistent(self) -> None:
        if not self._persist_path:
            return
        with self._persist_lock:
            with self._lock:
                data = []
                for task in self._heap:
                    if not task.persistent or task.cancelled:
                        continue
                    data.append({
                        "execute_at": task.execute_at,
                        "task_id": task.task_id,
                        "callback_name": task.callback_name,
                        "args": list(task.args),
                        "kwargs": task.kwargs,
                    })
            tmp_path = self._persist_path + ".tmp"
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, self._persist_path)
            except Exception:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

    def load_persistent(self) -> int:
        if not self._persist_path or not os.path.exists(self._persist_path):
            return 0
        loaded = 0
        with self._persist_lock:
            try:
                with open(self._persist_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                return 0
            with self._lock:
                for item in data:
                    try:
                        callback_name = item["callback_name"]
                        task = DelayTask(
                            execute_at=float(item["execute_at"]),
                            task_id=str(item["task_id"]),
                            callback=None,
                            callback_name=callback_name,
                            args=tuple(item.get("args", [])),
                            kwargs=dict(item.get("kwargs", {})),
                            persistent=True,
                        )
                        heapq.heappush(self._heap, task)
                        loaded += 1
                    except (KeyError, ValueError, TypeError):
                        continue
            if loaded > 0:
                self._event.set()
        return loaded

    def qsize(self) -> int:
        with self._lock:
            return len(self._heap)

    def __enter__(self) -> "DelayQueue":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
