import pytest
from agent_task_queue import TaskStatus, Task, TaskQueue


# ---------------------------------------------------------------------------
# Basic enqueue / dequeue
# ---------------------------------------------------------------------------

def test_enqueue_returns_task():
    q = TaskQueue()
    t = q.enqueue("search")
    assert isinstance(t, Task)
    assert t.name == "search"

def test_enqueue_default_priority():
    q = TaskQueue()
    t = q.enqueue("x")
    assert t.priority == 0

def test_enqueue_custom_priority():
    q = TaskQueue()
    t = q.enqueue("x", priority=5)
    assert t.priority == 5

def test_enqueue_payload():
    q = TaskQueue()
    t = q.enqueue("x", payload={"query": "cats"})
    assert t.payload == {"query": "cats"}

def test_enqueue_custom_id():
    q = TaskQueue()
    t = q.enqueue("x", task_id="myid")
    assert t.id == "myid"

def test_dequeue_returns_task():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    assert t is not None
    assert t.name == "x"

def test_dequeue_empty_returns_none():
    q = TaskQueue()
    assert q.dequeue() is None

def test_dequeue_sets_in_progress():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    assert t.status == TaskStatus.IN_PROGRESS

def test_dequeue_sets_started_at():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    assert t.started_at is not None

def test_dequeue_increments_attempts():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    assert t.attempts == 1


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------

def test_higher_priority_dequeued_first():
    q = TaskQueue()
    q.enqueue("low", priority=1)
    q.enqueue("high", priority=10)
    t = q.dequeue()
    assert t.name == "high"

def test_same_priority_fifo():
    q = TaskQueue()
    q.enqueue("first", priority=5)
    q.enqueue("second", priority=5)
    t1 = q.dequeue()
    t2 = q.dequeue()
    assert t1.name == "first"
    assert t2.name == "second"

def test_mixed_priorities():
    q = TaskQueue()
    q.enqueue("a", priority=1)
    q.enqueue("b", priority=3)
    q.enqueue("c", priority=2)
    names = []
    while not q.empty:
        t = q.dequeue()
        q.complete(t.id)
        names.append(t.name)
    assert names == ["b", "c", "a"]


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

def test_complete_sets_done():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.complete(t.id)
    assert t.status == TaskStatus.DONE

def test_complete_sets_finished_at():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.complete(t.id)
    assert t.finished_at is not None

def test_fail_sets_failed():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "oops")
    assert t.status == TaskStatus.FAILED
    assert t.error == "oops"

def test_fail_sets_finished_at():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "oops")
    assert t.finished_at is not None

def test_complete_unknown_raises():
    q = TaskQueue()
    with pytest.raises(KeyError):
        q.complete("nonexistent")

def test_fail_unknown_raises():
    q = TaskQueue()
    with pytest.raises(KeyError):
        q.fail("nonexistent")


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

def test_retry_failed_task():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "broken")
    q.retry(t.id)
    assert t.status == TaskStatus.PENDING
    assert t.error is None

def test_retry_makes_dequeue_work():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "broken")
    q.retry(t.id)
    t2 = q.dequeue()
    assert t2 is not None
    assert t2.id == t.id

def test_retry_non_failed_raises():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    with pytest.raises(ValueError):
        q.retry(t.id)  # IN_PROGRESS, not FAILED


# ---------------------------------------------------------------------------
# Auto-retry (max_retries)
# ---------------------------------------------------------------------------

def test_auto_retry_requeues_on_fail():
    q = TaskQueue(max_retries=1)
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "first failure")
    assert t.status == TaskStatus.PENDING   # re-queued

def test_auto_retry_exhausted_stays_failed():
    q = TaskQueue(max_retries=1)
    q.enqueue("x")
    t = q.dequeue()
    q.fail(t.id, "fail1")  # attempt 1 → requeued
    t2 = q.dequeue()
    assert t2 is not None
    q.fail(t2.id, "fail2")  # attempt 2 > max_retries(1) → FAILED
    assert t2.status == TaskStatus.FAILED


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

def test_cancel_pending_task():
    q = TaskQueue()
    t = q.enqueue("x")
    q.cancel(t.id)
    assert t.status == TaskStatus.FAILED
    assert t.error == "cancelled"

def test_cancel_removes_from_pending():
    q = TaskQueue()
    t = q.enqueue("x")
    q.cancel(t.id)
    assert q.empty

def test_cancel_in_progress_raises():
    q = TaskQueue()
    q.enqueue("x")
    t = q.dequeue()
    with pytest.raises(ValueError):
        q.cancel(t.id)


# ---------------------------------------------------------------------------
# Size / empty
# ---------------------------------------------------------------------------

def test_empty_on_new_queue():
    q = TaskQueue()
    assert q.empty
    assert q.size == 0

def test_not_empty_after_enqueue():
    q = TaskQueue()
    q.enqueue("x")
    assert not q.empty
    assert q.size == 1

def test_empty_after_all_dequeued_and_completed():
    q = TaskQueue()
    q.enqueue("a")
    q.enqueue("b")
    while True:
        t = q.dequeue()
        if t is None:
            break
        q.complete(t.id)
    assert q.empty


# ---------------------------------------------------------------------------
# Peek
# ---------------------------------------------------------------------------

def test_peek_returns_highest_priority():
    q = TaskQueue()
    q.enqueue("low", priority=1)
    q.enqueue("high", priority=5)
    t = q.peek()
    assert t is not None
    assert t.name == "high"

def test_peek_does_not_dequeue():
    q = TaskQueue()
    q.enqueue("x")
    q.peek()
    assert q.size == 1

def test_peek_empty_returns_none():
    q = TaskQueue()
    assert q.peek() is None


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

def test_all_tasks():
    q = TaskQueue()
    q.enqueue("a")
    q.enqueue("b")
    assert len(q.all_tasks()) == 2

def test_pending_list():
    q = TaskQueue()
    q.enqueue("a", priority=1)
    q.enqueue("b", priority=2)
    pending = q.pending()
    assert pending[0].name == "b"  # higher priority first

def test_in_progress_list():
    q = TaskQueue()
    q.enqueue("a")
    q.dequeue()
    assert len(q.in_progress()) == 1

def test_completed_list():
    q = TaskQueue()
    q.enqueue("a")
    t = q.dequeue()
    q.complete(t.id)
    assert len(q.completed()) == 1

def test_failed_tasks_list():
    q = TaskQueue()
    q.enqueue("a")
    t = q.dequeue()
    q.fail(t.id, "err")
    assert len(q.failed_tasks()) == 1

def test_get_by_id():
    q = TaskQueue()
    t = q.enqueue("x", task_id="myid")
    assert q.get("myid") is t

def test_get_missing_returns_none():
    q = TaskQueue()
    assert q.get("nothere") is None

def test_summary():
    q = TaskQueue()
    q.enqueue("a")
    q.enqueue("b")
    t = q.dequeue()
    q.complete(t.id)
    s = q.summary()
    assert s["pending"] == 1
    assert s["done"] == 1
    assert s["total"] == 2
