"""Unit tests for agent_task_queue.

Uses only the Python standard library (``unittest``) so the suite runs with
no third-party dependencies:

    python3 -m unittest discover -s tests
"""

import os
import sys
import unittest

# This project uses a ``src/`` layout, so the package is not importable from a
# fresh checkout unless it is installed (``pip install -e .``) or ``src/`` is on
# ``sys.path``. Add it here so the suite runs with the bare command above.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_task_queue import Task, TaskQueue, TaskStatus  # noqa: E402


# ---------------------------------------------------------------------------
# Basic enqueue / dequeue
# ---------------------------------------------------------------------------

class EnqueueDequeueTests(unittest.TestCase):
    def test_enqueue_returns_task(self):
        q = TaskQueue()
        t = q.enqueue("search")
        self.assertIsInstance(t, Task)
        self.assertEqual(t.name, "search")

    def test_enqueue_default_priority(self):
        q = TaskQueue()
        t = q.enqueue("x")
        self.assertEqual(t.priority, 0)

    def test_enqueue_custom_priority(self):
        q = TaskQueue()
        t = q.enqueue("x", priority=5)
        self.assertEqual(t.priority, 5)

    def test_enqueue_payload(self):
        q = TaskQueue()
        t = q.enqueue("x", payload={"query": "cats"})
        self.assertEqual(t.payload, {"query": "cats"})

    def test_enqueue_custom_id(self):
        q = TaskQueue()
        t = q.enqueue("x", task_id="myid")
        self.assertEqual(t.id, "myid")

    def test_enqueue_generates_unique_ids(self):
        q = TaskQueue()
        a = q.enqueue("a")
        b = q.enqueue("b")
        self.assertNotEqual(a.id, b.id)

    def test_enqueue_duplicate_id_raises(self):
        q = TaskQueue()
        q.enqueue("first", task_id="dup")
        with self.assertRaises(ValueError):
            q.enqueue("second", task_id="dup")

    def test_enqueue_duplicate_id_does_not_corrupt_queue(self):
        q = TaskQueue()
        q.enqueue("first", task_id="dup", priority=1)
        with self.assertRaises(ValueError):
            q.enqueue("second", task_id="dup", priority=5)
        # The original task is intact and still the only one.
        self.assertEqual(len(q.all_tasks()), 1)
        self.assertEqual(q.get("dup").name, "first")
        self.assertEqual(q.size, 1)

    def test_dequeue_returns_task(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        self.assertIsNotNone(t)
        self.assertEqual(t.name, "x")

    def test_dequeue_empty_returns_none(self):
        q = TaskQueue()
        self.assertIsNone(q.dequeue())

    def test_dequeue_sets_in_progress(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        self.assertEqual(t.status, TaskStatus.IN_PROGRESS)

    def test_dequeue_sets_started_at(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        self.assertIsNotNone(t.started_at)

    def test_dequeue_increments_attempts(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        self.assertEqual(t.attempts, 1)


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------

class PriorityOrderingTests(unittest.TestCase):
    def test_higher_priority_dequeued_first(self):
        q = TaskQueue()
        q.enqueue("low", priority=1)
        q.enqueue("high", priority=10)
        t = q.dequeue()
        self.assertEqual(t.name, "high")

    def test_same_priority_fifo(self):
        q = TaskQueue()
        q.enqueue("first", priority=5)
        q.enqueue("second", priority=5)
        t1 = q.dequeue()
        t2 = q.dequeue()
        self.assertEqual(t1.name, "first")
        self.assertEqual(t2.name, "second")

    def test_mixed_priorities(self):
        q = TaskQueue()
        q.enqueue("a", priority=1)
        q.enqueue("b", priority=3)
        q.enqueue("c", priority=2)
        names = []
        while not q.empty:
            t = q.dequeue()
            q.complete(t.id)
            names.append(t.name)
        self.assertEqual(names, ["b", "c", "a"])

    def test_negative_priorities(self):
        q = TaskQueue()
        q.enqueue("normal", priority=0)
        q.enqueue("deprioritized", priority=-5)
        self.assertEqual(q.dequeue().name, "normal")
        self.assertEqual(q.dequeue().name, "deprioritized")


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

class StatusTransitionTests(unittest.TestCase):
    def test_complete_sets_done(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.complete(t.id)
        self.assertEqual(t.status, TaskStatus.DONE)
        self.assertTrue(t.done)

    def test_complete_sets_finished_at(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.complete(t.id)
        self.assertIsNotNone(t.finished_at)

    def test_fail_sets_failed(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "oops")
        self.assertEqual(t.status, TaskStatus.FAILED)
        self.assertEqual(t.error, "oops")
        self.assertTrue(t.failed)

    def test_fail_sets_finished_at(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "oops")
        self.assertIsNotNone(t.finished_at)

    def test_complete_unknown_raises(self):
        q = TaskQueue()
        with self.assertRaises(KeyError):
            q.complete("nonexistent")

    def test_fail_unknown_raises(self):
        q = TaskQueue()
        with self.assertRaises(KeyError):
            q.fail("nonexistent")

    def test_status_flag_properties(self):
        q = TaskQueue()
        t = q.enqueue("x")
        self.assertTrue(t.pending)
        q.dequeue()
        self.assertTrue(t.in_progress)


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

class RetryTests(unittest.TestCase):
    def test_retry_failed_task(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "broken")
        q.retry(t.id)
        self.assertEqual(t.status, TaskStatus.PENDING)
        self.assertIsNone(t.error)

    def test_retry_makes_dequeue_work(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "broken")
        q.retry(t.id)
        t2 = q.dequeue()
        self.assertIsNotNone(t2)
        self.assertEqual(t2.id, t.id)

    def test_retry_non_failed_raises(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        with self.assertRaises(ValueError):
            q.retry(t.id)  # IN_PROGRESS, not FAILED

    def test_retry_clears_finished_at(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "broken")
        q.retry(t.id)
        self.assertIsNone(t.finished_at)
        self.assertIsNone(t.started_at)


# ---------------------------------------------------------------------------
# Auto-retry (max_retries)
# ---------------------------------------------------------------------------

class AutoRetryTests(unittest.TestCase):
    def test_auto_retry_requeues_on_fail(self):
        q = TaskQueue(max_retries=1)
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "first failure")
        self.assertEqual(t.status, TaskStatus.PENDING)  # re-queued

    def test_auto_retry_exhausted_stays_failed(self):
        q = TaskQueue(max_retries=1)
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "fail1")  # attempt 1 -> requeued
        t2 = q.dequeue()
        self.assertIsNotNone(t2)
        q.fail(t2.id, "fail2")  # attempt 2 > max_retries(1) -> FAILED
        self.assertEqual(t2.status, TaskStatus.FAILED)

    def test_no_auto_retry_by_default(self):
        q = TaskQueue()  # max_retries=0
        q.enqueue("x")
        t = q.dequeue()
        q.fail(t.id, "boom")
        self.assertEqual(t.status, TaskStatus.FAILED)


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

class CancelTests(unittest.TestCase):
    def test_cancel_pending_task(self):
        q = TaskQueue()
        t = q.enqueue("x")
        q.cancel(t.id)
        self.assertEqual(t.status, TaskStatus.FAILED)
        self.assertEqual(t.error, "cancelled")

    def test_cancel_removes_from_pending(self):
        q = TaskQueue()
        t = q.enqueue("x")
        q.cancel(t.id)
        self.assertTrue(q.empty)

    def test_cancel_in_progress_raises(self):
        q = TaskQueue()
        q.enqueue("x")
        t = q.dequeue()
        with self.assertRaises(ValueError):
            q.cancel(t.id)

    def test_cancel_leaves_other_tasks_dequeuable(self):
        q = TaskQueue()
        a = q.enqueue("a", priority=1)
        q.enqueue("b", priority=2)
        q.cancel(a.id)
        self.assertEqual(q.dequeue().name, "b")
        self.assertIsNone(q.dequeue())


# ---------------------------------------------------------------------------
# Size / empty
# ---------------------------------------------------------------------------

class SizeEmptyTests(unittest.TestCase):
    def test_empty_on_new_queue(self):
        q = TaskQueue()
        self.assertTrue(q.empty)
        self.assertEqual(q.size, 0)

    def test_not_empty_after_enqueue(self):
        q = TaskQueue()
        q.enqueue("x")
        self.assertFalse(q.empty)
        self.assertEqual(q.size, 1)

    def test_empty_after_all_dequeued_and_completed(self):
        q = TaskQueue()
        q.enqueue("a")
        q.enqueue("b")
        while True:
            t = q.dequeue()
            if t is None:
                break
            q.complete(t.id)
        self.assertTrue(q.empty)


# ---------------------------------------------------------------------------
# Peek
# ---------------------------------------------------------------------------

class PeekTests(unittest.TestCase):
    def test_peek_returns_highest_priority(self):
        q = TaskQueue()
        q.enqueue("low", priority=1)
        q.enqueue("high", priority=5)
        t = q.peek()
        self.assertIsNotNone(t)
        self.assertEqual(t.name, "high")

    def test_peek_does_not_dequeue(self):
        q = TaskQueue()
        q.enqueue("x")
        q.peek()
        self.assertEqual(q.size, 1)

    def test_peek_empty_returns_none(self):
        q = TaskQueue()
        self.assertIsNone(q.peek())


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

class IntrospectionTests(unittest.TestCase):
    def test_all_tasks(self):
        q = TaskQueue()
        q.enqueue("a")
        q.enqueue("b")
        self.assertEqual(len(q.all_tasks()), 2)

    def test_pending_list(self):
        q = TaskQueue()
        q.enqueue("a", priority=1)
        q.enqueue("b", priority=2)
        pending = q.pending()
        self.assertEqual(pending[0].name, "b")  # higher priority first

    def test_in_progress_list(self):
        q = TaskQueue()
        q.enqueue("a")
        q.dequeue()
        self.assertEqual(len(q.in_progress()), 1)

    def test_completed_list(self):
        q = TaskQueue()
        q.enqueue("a")
        t = q.dequeue()
        q.complete(t.id)
        self.assertEqual(len(q.completed()), 1)

    def test_failed_tasks_list(self):
        q = TaskQueue()
        q.enqueue("a")
        t = q.dequeue()
        q.fail(t.id, "err")
        self.assertEqual(len(q.failed_tasks()), 1)

    def test_get_by_id(self):
        q = TaskQueue()
        t = q.enqueue("x", task_id="myid")
        self.assertIs(q.get("myid"), t)

    def test_get_missing_returns_none(self):
        q = TaskQueue()
        self.assertIsNone(q.get("nothere"))

    def test_summary(self):
        q = TaskQueue()
        q.enqueue("a")
        q.enqueue("b")
        t = q.dequeue()
        q.complete(t.id)
        s = q.summary()
        self.assertEqual(s["pending"], 1)
        self.assertEqual(s["done"], 1)
        self.assertEqual(s["total"], 2)

    def test_summary_has_all_statuses(self):
        q = TaskQueue()
        s = q.summary()
        for status in TaskStatus:
            self.assertIn(status.value, s)
        self.assertEqual(s["total"], 0)


# ---------------------------------------------------------------------------
# End-to-end agent-loop style workflow
# ---------------------------------------------------------------------------

class WorkflowTests(unittest.TestCase):
    def test_drain_loop(self):
        q = TaskQueue()
        q.enqueue("search web", priority=2, payload={"query": "cats"})
        q.enqueue("summarize results", priority=1)

        processed = []
        while not q.empty:
            task = q.dequeue()
            processed.append(task.name)
            q.complete(task.id)

        self.assertEqual(processed, ["search web", "summarize results"])
        self.assertEqual(q.summary()["done"], 2)
        self.assertTrue(q.empty)


if __name__ == "__main__":
    unittest.main()
