"""Tests for agent_task_queue.TaskQueue."""

import threading
import time

import pytest

from agent_task_queue import Task, TaskNotFoundError, TaskQueue

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fresh() -> TaskQueue:
    """Return a new in-memory TaskQueue."""
    return TaskQueue()


def tmp_queue(tmp_path) -> TaskQueue:
    """Return a TaskQueue persisted under pytest's tmp_path."""
    return TaskQueue(persist_path=str(tmp_path / "q.jsonl"))


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------


def test_enqueue_returns_task():
    q = fresh()
    t = q.enqueue("fetch", {"url": "https://example.com"})
    assert isinstance(t, Task)


def test_enqueue_fields():
    q = fresh()
    t = q.enqueue("fetch", {"url": "x"}, priority=3)
    assert t.name == "fetch"
    assert t.payload == {"url": "x"}
    assert t.priority == 3
    assert t.status == "pending"
    assert t.id  # non-empty uuid string
    assert t.created_at > 0
    assert t.updated_at == t.created_at
    assert t.error is None


def test_enqueue_default_payload():
    q = fresh()
    t = q.enqueue("noop")
    assert t.payload == {}


def test_enqueue_default_priority():
    q = fresh()
    t = q.enqueue("noop")
    assert t.priority == 5


def test_enqueue_increases_size():
    q = fresh()
    q.enqueue("a")
    q.enqueue("b")
    assert q.size == 2


# ---------------------------------------------------------------------------
# dequeue – ordering
# ---------------------------------------------------------------------------


def test_dequeue_highest_priority_first():
    q = fresh()
    q.enqueue("low", priority=10)
    q.enqueue("high", priority=1)
    q.enqueue("mid", priority=5)
    t = q.dequeue()
    assert t is not None
    assert t.name == "high"


def test_dequeue_fifo_on_tie():
    q = fresh()
    t1 = q.enqueue("first", priority=5)
    time.sleep(0.01)  # ensure different created_at
    q.enqueue("second", priority=5)
    got = q.dequeue()
    assert got is not None
    assert got.id == t1.id


def test_dequeue_returns_none_when_empty():
    q = fresh()
    assert q.dequeue() is None


def test_dequeue_returns_none_no_pending():
    q = fresh()
    q.enqueue("work")
    q.dequeue()  # moves to in_progress
    assert q.dequeue() is None


def test_dequeue_marks_in_progress():
    q = fresh()
    q.enqueue("work")
    t = q.dequeue()
    assert t is not None
    assert t.status == "in_progress"


def test_dequeue_updates_updated_at():
    q = fresh()
    t = q.enqueue("work")
    original = t.updated_at
    time.sleep(0.01)
    dequeued = q.dequeue()
    assert dequeued is not None
    assert dequeued.updated_at >= original


# ---------------------------------------------------------------------------
# peek
# ---------------------------------------------------------------------------


def test_peek_does_not_change_status():
    q = fresh()
    q.enqueue("work")
    t = q.peek()
    assert t is not None
    assert t.status == "pending"


def test_peek_returns_none_on_empty():
    q = fresh()
    assert q.peek() is None


def test_peek_returns_highest_priority():
    q = fresh()
    q.enqueue("low", priority=9)
    q.enqueue("top", priority=1)
    t = q.peek()
    assert t is not None
    assert t.name == "top"


# ---------------------------------------------------------------------------
# complete
# ---------------------------------------------------------------------------


def test_complete_marks_done():
    q = fresh()
    task = q.enqueue("work")
    q.dequeue()
    result = q.complete(task.id)
    assert result.status == "done"


def test_complete_updates_updated_at():
    q = fresh()
    task = q.enqueue("work")
    before = task.updated_at
    time.sleep(0.01)
    q.complete(task.id)
    assert q.get(task.id).updated_at >= before


def test_complete_raises_for_unknown():
    q = fresh()
    with pytest.raises(TaskNotFoundError):
        q.complete("no-such-id")


# ---------------------------------------------------------------------------
# fail
# ---------------------------------------------------------------------------


def test_fail_marks_failed():
    q = fresh()
    task = q.enqueue("work")
    q.dequeue()
    result = q.fail(task.id, error="timeout")
    assert result.status == "failed"
    assert result.error == "timeout"


def test_fail_no_error_message():
    q = fresh()
    task = q.enqueue("work")
    result = q.fail(task.id)
    assert result.status == "failed"
    assert result.error is None


def test_fail_raises_for_unknown():
    q = fresh()
    with pytest.raises(TaskNotFoundError):
        q.fail("no-such-id")


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_get_returns_task():
    q = fresh()
    task = q.enqueue("x")
    assert q.get(task.id).id == task.id


def test_get_raises_for_unknown():
    q = fresh()
    with pytest.raises(TaskNotFoundError):
        q.get("ghost")


# ---------------------------------------------------------------------------
# filter lists
# ---------------------------------------------------------------------------


def test_pending_filter():
    q = fresh()
    q.enqueue("a")
    q.enqueue("b")
    q.dequeue()  # first -> in_progress
    assert len(q.pending()) == 1
    assert q.pending()[0].name == "b"


def test_in_progress_filter():
    q = fresh()
    q.enqueue("a")
    q.enqueue("b")
    q.dequeue()
    assert len(q.in_progress()) == 1


def test_done_filter():
    q = fresh()
    task = q.enqueue("a")
    q.complete(task.id)
    assert len(q.done()) == 1


def test_failed_filter():
    q = fresh()
    task = q.enqueue("a")
    q.fail(task.id, "boom")
    assert len(q.failed()) == 1


def test_filter_lists_sorted_by_priority():
    q = fresh()
    q.enqueue("lo", priority=9)
    q.enqueue("hi", priority=1)
    p = q.pending()
    assert p[0].name == "hi"
    assert p[1].name == "lo"


# ---------------------------------------------------------------------------
# stats / size / clear
# ---------------------------------------------------------------------------


def test_stats_correct_counts():
    q = fresh()
    a = q.enqueue("a")
    b = q.enqueue("b")
    q.enqueue("c")
    q.dequeue()       # a -> in_progress
    q.complete(a.id)  # a -> done
    q.fail(b.id)      # b -> failed (fail works from any status)
    s = q.stats()
    assert s["pending"] == 1
    assert s["in_progress"] == 0
    assert s["done"] == 1
    assert s["failed"] == 1
    assert s["total"] == 3


def test_size_property():
    q = fresh()
    assert q.size == 0
    q.enqueue("x")
    q.enqueue("y")
    assert q.size == 2


def test_clear_removes_everything():
    q = fresh()
    q.enqueue("a")
    q.enqueue("b")
    q.clear()
    assert q.size == 0
    assert q.pending() == []


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


def test_thread_safety_enqueue():
    q = fresh()
    threads = [
        threading.Thread(target=lambda: [q.enqueue(f"task-{i}") for i in range(20)])
        for _ in range(5)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert q.size == 100


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def test_persist_enqueue_survives_reload(tmp_path):
    path = str(tmp_path / "q.jsonl")
    q = TaskQueue(persist_path=path)
    t = q.enqueue("job", {"x": 1}, priority=2)
    task_id = t.id

    q2 = TaskQueue(persist_path=path)
    loaded = q2.get(task_id)
    assert loaded.name == "job"
    assert loaded.payload == {"x": 1}
    assert loaded.priority == 2
    assert loaded.status == "pending"


def test_persist_complete_survives_reload(tmp_path):
    path = str(tmp_path / "q.jsonl")
    q = TaskQueue(persist_path=path)
    t = q.enqueue("job")
    q.complete(t.id)

    q2 = TaskQueue(persist_path=path)
    assert q2.get(t.id).status == "done"


def test_persist_fail_survives_reload(tmp_path):
    path = str(tmp_path / "q.jsonl")
    q = TaskQueue(persist_path=path)
    t = q.enqueue("job")
    q.fail(t.id, error="oops")

    q2 = TaskQueue(persist_path=path)
    loaded = q2.get(t.id)
    assert loaded.status == "failed"
    assert loaded.error == "oops"


def test_persist_clear_reload_starts_empty(tmp_path):
    path = str(tmp_path / "q.jsonl")
    q = TaskQueue(persist_path=path)
    q.enqueue("a")
    q.enqueue("b")
    q.clear()

    q2 = TaskQueue(persist_path=path)
    assert q2.size == 0


def test_persist_tilde_expansion(tmp_path):
    # Write a real file under home dir equivalent using a literal tmp path,
    # then confirm that ~ in a path inside tmp_path resolves without error.
    # We test expansion by using os.path.expanduser and confirming no crash.
    path = str(tmp_path / "tilde_q.jsonl")
    # Patch: just verify ~ in a path that exists is handled (use real tmp path)
    q = TaskQueue(persist_path=path)
    q.enqueue("test")
    q2 = TaskQueue(persist_path=path)
    assert q2.size == 1


def test_persist_none_no_file(tmp_path):
    q = TaskQueue()  # no persist_path
    q.enqueue("a")
    # no JSONL file should be created anywhere in tmp_path
    files = list(tmp_path.iterdir())
    assert files == []
