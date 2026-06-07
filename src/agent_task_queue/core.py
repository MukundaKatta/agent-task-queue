"""
agent_task_queue — priority queue for sub-tasks in agent loops.

Enqueue named tasks with priorities, dequeue in order, track status,
retry failures. Zero dependencies (stdlib: heapq, uuid, time, dataclasses).
"""

from __future__ import annotations

import heapq
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

@dataclass
class Task:
    """A single queued task."""

    id: str
    name: str
    priority: int = 0          # higher = more urgent
    payload: Any = None
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    attempts: int = 0

    @property
    def done(self) -> bool:
        return self.status == TaskStatus.DONE

    @property
    def failed(self) -> bool:
        return self.status == TaskStatus.FAILED

    @property
    def pending(self) -> bool:
        return self.status == TaskStatus.PENDING

    @property
    def in_progress(self) -> bool:
        return self.status == TaskStatus.IN_PROGRESS

    def __repr__(self) -> str:
        return (
            f"Task(id={self.id[:8]!r}, name={self.name!r}, "
            f"priority={self.priority}, status={self.status.value!r})"
        )


# ---------------------------------------------------------------------------
# Internal heap entry
# ---------------------------------------------------------------------------

@dataclass(order=True)
class _HeapEntry:
    """Heap entry: higher priority wins; tie-break by insertion order."""
    neg_priority: int          # negated so heapq (min-heap) gives highest first
    seq: int                   # insertion sequence for FIFO tie-breaking
    task_id: str = field(compare=False)


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------

class TaskQueue:
    """
    Priority queue for agent sub-tasks.

    Usage::

        q = TaskQueue()
        q.enqueue("search web", priority=2, payload={"query": "cats"})
        q.enqueue("summarize", priority=1)

        while not q.empty:
            task = q.dequeue()
            try:
                run_task(task)
                q.complete(task.id)
            except Exception as e:
                q.fail(task.id, str(e))
    """

    def __init__(self, *, max_retries: int = 0) -> None:
        """
        Args:
            max_retries: How many times a failed task is automatically
                         re-enqueued before staying FAILED. Default 0 (no auto-retry).
        """
        self._tasks: dict[str, Task] = {}
        self._heap: list[_HeapEntry] = []
        self._seq = 0
        self.max_retries = max_retries

    # ------------------------------------------------------------------
    # Enqueueing
    # ------------------------------------------------------------------

    def enqueue(
        self,
        name: str,
        *,
        priority: int = 0,
        payload: Any = None,
        task_id: str | None = None,
    ) -> Task:
        """
        Add a new task to the queue.

        Args:
            name: Human-readable task name.
            priority: Higher numbers = higher priority (default 0).
            payload: Arbitrary data for the task handler.
            task_id: Custom ID; generated if not provided.

        Returns:
            The new :class:`Task`.

        Raises:
            ValueError: If ``task_id`` is supplied and a task with that ID
                already exists. (Silently overwriting it would orphan the
                old heap entry and corrupt the queue's accounting.)
        """
        tid = task_id or uuid.uuid4().hex
        if tid in self._tasks:
            raise ValueError(f"A task with id {tid!r} already exists")
        task = Task(id=tid, name=name, priority=priority, payload=payload)
        self._tasks[tid] = task
        self._push_heap(task)
        return task

    def _push_heap(self, task: Task) -> None:
        entry = _HeapEntry(neg_priority=-task.priority, seq=self._seq, task_id=task.id)
        self._seq += 1
        heapq.heappush(self._heap, entry)

    # ------------------------------------------------------------------
    # Dequeueing
    # ------------------------------------------------------------------

    def dequeue(self) -> Task | None:
        """
        Remove and return the highest-priority pending task.

        Returns:
            :class:`Task`, or ``None`` if no pending tasks.
        """
        while self._heap:
            entry = heapq.heappop(self._heap)
            task = self._tasks.get(entry.task_id)
            if task is None or task.status != TaskStatus.PENDING:
                continue  # stale heap entry
            task.status = TaskStatus.IN_PROGRESS
            task.started_at = time.time()
            task.attempts += 1
            return task
        return None

    def peek(self) -> Task | None:
        """
        Return the highest-priority pending task without removing it.

        Returns:
            :class:`Task` or ``None``.
        """
        # Walk the heap to find the first still-pending entry
        for entry in sorted(self._heap):
            task = self._tasks.get(entry.task_id)
            if task and task.status == TaskStatus.PENDING:
                return task
        return None

    # ------------------------------------------------------------------
    # Status transitions
    # ------------------------------------------------------------------

    def complete(self, task_id: str) -> Task:
        """
        Mark a task as DONE.

        Args:
            task_id: Task ID.

        Returns:
            Updated :class:`Task`.

        Raises:
            KeyError: If task not found.
        """
        task = self._get(task_id)
        task.status = TaskStatus.DONE
        task.finished_at = time.time()
        return task

    def fail(self, task_id: str, error: str = "") -> Task:
        """
        Mark a task as FAILED.

        If ``max_retries`` allows, the task is automatically re-enqueued.

        Args:
            task_id: Task ID.
            error: Error description.

        Returns:
            Updated :class:`Task`.
        """
        task = self._get(task_id)
        task.error = error
        if task.attempts <= self.max_retries:
            # Re-enqueue for retry
            task.status = TaskStatus.PENDING
            task.started_at = None
            self._push_heap(task)
        else:
            task.status = TaskStatus.FAILED
            task.finished_at = time.time()
        return task

    def retry(self, task_id: str) -> Task:
        """
        Manually re-enqueue a FAILED task.

        Args:
            task_id: Task ID.

        Returns:
            Updated :class:`Task`.

        Raises:
            ValueError: If the task is not in FAILED status.
        """
        task = self._get(task_id)
        if task.status != TaskStatus.FAILED:
            raise ValueError(f"Task {task_id!r} is not FAILED (status={task.status.value!r})")
        task.status = TaskStatus.PENDING
        task.error = None
        task.started_at = None
        task.finished_at = None
        self._push_heap(task)
        return task

    def cancel(self, task_id: str) -> Task:
        """
        Remove a PENDING task from the queue.

        The task record remains; its status is set to FAILED with
        ``error="cancelled"``.

        Args:
            task_id: Task ID.

        Returns:
            Updated :class:`Task`.
        """
        task = self._get(task_id)
        if task.status != TaskStatus.PENDING:
            raise ValueError(f"Can only cancel PENDING tasks (status={task.status.value!r})")
        task.status = TaskStatus.FAILED
        task.error = "cancelled"
        task.finished_at = time.time()
        return task

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def empty(self) -> bool:
        """True if no PENDING tasks remain."""
        return self._pending_count() == 0

    @property
    def size(self) -> int:
        """Number of PENDING tasks."""
        return self._pending_count()

    def _pending_count(self) -> int:
        return sum(1 for t in self._tasks.values() if t.status == TaskStatus.PENDING)

    def all_tasks(self) -> list[Task]:
        """Return all tasks (any status) in insertion order."""
        return list(self._tasks.values())

    def pending(self) -> list[Task]:
        """Return all PENDING tasks, sorted by priority descending."""
        tasks = [t for t in self._tasks.values() if t.status == TaskStatus.PENDING]
        return sorted(tasks, key=lambda t: (-t.priority, t.created_at))

    def in_progress(self) -> list[Task]:
        return [t for t in self._tasks.values() if t.status == TaskStatus.IN_PROGRESS]

    def completed(self) -> list[Task]:
        return [t for t in self._tasks.values() if t.status == TaskStatus.DONE]

    def failed_tasks(self) -> list[Task]:
        return [t for t in self._tasks.values() if t.status == TaskStatus.FAILED]

    def get(self, task_id: str) -> Task | None:
        """Return a task by ID, or None."""
        return self._tasks.get(task_id)

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {s.value: 0 for s in TaskStatus}
        for t in self._tasks.values():
            counts[t.status.value] += 1
        counts["total"] = len(self._tasks)
        return counts

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, task_id: str) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"No task with id {task_id!r}")
        return task
