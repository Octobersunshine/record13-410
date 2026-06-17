import heapq
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Any, Optional


@dataclass(order=True)
class DelayTask:
    execute_at: float
    task_id: str = field(compare=False)
    callback: Callable = field(compare=False)
    args: tuple = field(compare=False, default_factory=tuple)
    kwargs: dict = field(compare=False, default_factory=dict)
    cancelled: bool = field(compare=False, default=False)


class DelayQueue:
    def __init__(self):
        self._heap: list[DelayTask] = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._running = False
        self._thread: Optional[threading.Thread] = None

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

    def submit(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> str:
        if delay < 0:
            raise ValueError("delay must be non-negative")
        task_id = uuid.uuid4().hex
        task = DelayTask(
            execute_at=time.time() + delay,
            task_id=task_id,
            callback=callback,
            args=args,
            kwargs=kwargs,
        )
        with self._lock:
            heapq.heappush(self._heap, task)
            self._event.set()
        return task_id

    def cancel(self, task_id: str) -> bool:
        with self._lock:
            for task in self._heap:
                if task.task_id == task_id:
                    task.cancelled = True
                    return True
        return False

    def _run(self) -> None:
        while self._running:
            self._event.wait()
            self._event.clear()
            while self._running:
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
                if not task.cancelled:
                    try:
                        task.callback(*task.args, **task.kwargs)
                    except Exception:
                        pass

    def qsize(self) -> int:
        with self._lock:
            return len(self._heap)

    def __enter__(self) -> "DelayQueue":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
