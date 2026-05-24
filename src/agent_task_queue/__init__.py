"""agent-task-queue: priority queue for agent sub-tasks with optional JSONL persistence."""

import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Literal

STATUS = Literal["pending", "in_progress", "done", "failed"]


@dataclass
class Task:
    id: str
    name: str
    payload: dict
    priority: int  # lower number = higher priority; default 5
    status: STATUS
    created_at: float  # wall-clock time (time.time()) for persistence
    updated_at: float
    error: str | None = None  # set on fail()


class QueueEmptyError(Exception):
    """Raised when dequeue is called on an empty queue (no pending tasks)."""


class TaskNotFoundError(Exception):
    """Raised when a task_id cannot be found in the queue."""


def _sort_key(task: Task) -> tuple:
    """Sort key: lowest priority number first, then FIFO by created_at."""
    return (task.priority, task.created_at)


class TaskQueue:
    """Priority queue for agent sub-tasks.

    Tasks with lower ``priority`` numbers are dequeued first.  Ties are broken
    by ``created_at`` (FIFO).  All public methods are thread-safe via an RLock.

    If ``persist_path`` is given the queue is persisted as a JSONL file.  Each
    mutation appends one line; on reload the last line for each task_id wins.
    A ``{"_cleared": true}`` sentinel resets state during reload.
    """

    def __init__(self, persist_path: str | None = None) -> None:
        self._lock = threading.RLock()
        # Ordered dict preserves insertion order for iteration; keyed by task id.
        self._tasks: dict[str, Task] = {}
        self._persist_path: str | None = None

        if persist_path is not None:
            expanded = os.path.expanduser(persist_path)
            self._persist_path = expanded
            self._load()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Replay the JSONL file to reconstruct queue state."""
        path = self._persist_path
        if path is None or not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("_cleared"):
                    self._tasks.clear()
                    continue
                task = Task(
                    id=record["id"],
                    name=record["name"],
                    payload=record["payload"],
                    priority=record["priority"],
                    status=record["status"],
                    created_at=record["created_at"],
                    updated_at=record["updated_at"],
                    error=record.get("error"),
                )
                self._tasks[task.id] = task

    def _append(self, task: Task) -> None:
        """Append a single task record to the JSONL file."""
        if self._persist_path is None:
            return
        with open(self._persist_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(asdict(task)) + "\n")

    def _append_cleared(self) -> None:
        """Append the clear sentinel to the JSONL file."""
        if self._persist_path is None:
            return
        with open(self._persist_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"_cleared": True}) + "\n")

    def _pending_sorted(self) -> list[Task]:
        """Return pending tasks sorted by priority then created_at."""
        return sorted(
            (t for t in self._tasks.values() if t.status == "pending"),
            key=_sort_key,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, name: str, payload: dict | None = None, priority: int = 5) -> Task:
        """Add a new task to the queue.

        Args:
            name: Human-readable task name.
            payload: Arbitrary dict of task data (defaults to ``{}``).
            priority: Lower number = higher priority (default ``5``).

        Returns:
            The newly created :class:`Task`.
        """
        if payload is None:
            payload = {}
        now = time.time()
        task = Task(
            id=str(uuid.uuid4()),
            name=name,
            payload=payload,
            priority=priority,
            status="pending",
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._tasks[task.id] = task
            self._append(task)
        return task

    def dequeue(self) -> Task | None:
        """Return the highest-priority pending task and mark it in_progress.

        Ties are broken by ``created_at`` (FIFO).

        Returns:
            The next :class:`Task`, or ``None`` if no pending tasks exist.
        """
        with self._lock:
            candidates = self._pending_sorted()
            if not candidates:
                return None
            task = candidates[0]
            task.status = "in_progress"
            task.updated_at = time.time()
            self._append(task)
        return task

    def peek(self) -> Task | None:
        """Return the highest-priority pending task WITHOUT changing its status.

        Returns:
            The next :class:`Task`, or ``None`` if no pending tasks exist.
        """
        with self._lock:
            candidates = self._pending_sorted()
            return candidates[0] if candidates else None

    def complete(self, task_id: str) -> Task:
        """Mark a task as done.

        Args:
            task_id: The :attr:`Task.id` to complete.

        Returns:
            The updated :class:`Task`.

        Raises:
            TaskNotFoundError: If *task_id* is not in the queue.
        """
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(task_id)
            task = self._tasks[task_id]
            task.status = "done"
            task.updated_at = time.time()
            self._append(task)
        return task

    def fail(self, task_id: str, error: str | None = None) -> Task:
        """Mark a task as failed and optionally record an error message.

        Args:
            task_id: The :attr:`Task.id` to fail.
            error: Optional error description.

        Returns:
            The updated :class:`Task`.

        Raises:
            TaskNotFoundError: If *task_id* is not in the queue.
        """
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(task_id)
            task = self._tasks[task_id]
            task.status = "failed"
            task.error = error
            task.updated_at = time.time()
            self._append(task)
        return task

    def get(self, task_id: str) -> Task:
        """Retrieve a task by id.

        Raises:
            TaskNotFoundError: If *task_id* is not in the queue.
        """
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(task_id)
            return self._tasks[task_id]

    def pending(self) -> list[Task]:
        """Return a sorted list of all pending tasks (priority then created_at)."""
        with self._lock:
            return sorted(
                [t for t in self._tasks.values() if t.status == "pending"],
                key=_sort_key,
            )

    def in_progress(self) -> list[Task]:
        """Return a sorted list of all in-progress tasks."""
        with self._lock:
            return sorted(
                [t for t in self._tasks.values() if t.status == "in_progress"],
                key=_sort_key,
            )

    def done(self) -> list[Task]:
        """Return a sorted list of all completed tasks."""
        with self._lock:
            return sorted(
                [t for t in self._tasks.values() if t.status == "done"],
                key=_sort_key,
            )

    def failed(self) -> list[Task]:
        """Return a sorted list of all failed tasks."""
        with self._lock:
            return sorted(
                [t for t in self._tasks.values() if t.status == "failed"],
                key=_sort_key,
            )

    @property
    def size(self) -> int:
        """Total number of tasks (all statuses)."""
        with self._lock:
            return len(self._tasks)

    def stats(self) -> dict:
        """Return a dict with counts for each status and a total.

        Returns:
            ``{"pending": N, "in_progress": N, "done": N, "failed": N, "total": N}``
        """
        with self._lock:
            counts: dict[str, int] = {"pending": 0, "in_progress": 0, "done": 0, "failed": 0}
            for task in self._tasks.values():
                counts[task.status] += 1
            counts["total"] = len(self._tasks)
            return counts

    def clear(self) -> None:
        """Remove all tasks from the queue and write a clear sentinel if persisting."""
        with self._lock:
            self._tasks.clear()
            self._append_cleared()


__version__ = "0.1.0"

__all__ = [
    "STATUS",
    "Task",
    "QueueEmptyError",
    "TaskNotFoundError",
    "TaskQueue",
    "__version__",
]
