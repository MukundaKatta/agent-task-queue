# agent-task-queue

[![CI](https://github.com/MukundaKatta/agent-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/MukundaKatta/agent-task-queue/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A tiny, dependency-free **priority queue for the sub-tasks an agent loop spawns**.

When an LLM agent decides to do several things ("search the web", then "summarize",
then "write the answer"), you need somewhere to keep that work, run it in the right
order, and track which steps finished, failed, or need a retry. `agent-task-queue`
is exactly that — a single `TaskQueue` class backed by `heapq`, with no third-party
dependencies (stdlib only: `heapq`, `uuid`, `time`, `dataclasses`, `enum`).

## Features

- **Priority ordering** — higher `priority` runs first; ties break FIFO (insertion order).
- **Status tracking** — every task moves through `PENDING -> IN_PROGRESS -> DONE/FAILED`.
- **Manual and automatic retries** — re-queue a failed task by hand, or set
  `max_retries` to have failures re-queued automatically until exhausted.
- **Cancellation** — drop a still-pending task out of the queue.
- **Introspection** — `peek`, `summary`, and per-status listings for dashboards/logging.
- **Zero dependencies**, fully type-hinted, ships a `py.typed` marker.

## Install

```bash
pip install agent-task-queue
```

Requires Python 3.10 or newer.

## Quick start

```python
from agent_task_queue import TaskQueue

q = TaskQueue()
q.enqueue("search web", priority=2, payload={"query": "cats"})
q.enqueue("summarize results", priority=1)

while not q.empty:
    task = q.dequeue()          # highest priority first
    try:
        result = run(task)      # your handler; task.payload has the data
        q.complete(task.id)
    except Exception as e:
        q.fail(task.id, str(e))

print(q.summary())              # {'pending': 0, 'in_progress': 0, 'done': 2, ...}
```

### Automatic retries

```python
q = TaskQueue(max_retries=2)    # each task may fail up to 3 times total
q.enqueue("flaky api call")

task = q.dequeue()
q.fail(task.id, "timeout")      # attempt 1 -> automatically re-queued
# ... dequeue + fail again -> attempt 2 re-queued
# ... third failure -> task stays FAILED
```

### Manual retry and cancel

```python
q = TaskQueue()
t = q.enqueue("risky step")

task = q.dequeue()
q.fail(task.id, "boom")
q.retry(task.id)                # FAILED -> PENDING again

other = q.enqueue("never mind")
q.cancel(other.id)              # drop a still-pending task
```

## API reference

### `TaskQueue(*, max_retries=0)`

| Method / property              | Description                                                                 |
| ------------------------------ | --------------------------------------------------------------------------- |
| `enqueue(name, *, priority=0, payload=None, task_id=None) -> Task` | Add a task. Raises `ValueError` if `task_id` is already in use. |
| `dequeue() -> Task \| None`    | Remove and return the highest-priority pending task (sets it `IN_PROGRESS`); `None` if none pending. |
| `peek() -> Task \| None`       | Look at the next task without removing it.                                   |
| `complete(task_id) -> Task`    | Mark a task `DONE`. Raises `KeyError` if unknown.                            |
| `fail(task_id, error="") -> Task` | Mark `FAILED`; auto-re-queues while `attempts <= max_retries`.            |
| `retry(task_id) -> Task`       | Re-queue a `FAILED` task. Raises `ValueError` if it is not `FAILED`.         |
| `cancel(task_id) -> Task`      | Drop a `PENDING` task (marks it `FAILED` with `error="cancelled"`). Raises `ValueError` otherwise. |
| `empty` / `size`               | Whether any tasks are pending / how many are pending.                        |
| `get(task_id) -> Task \| None` | Fetch a task by id.                                                          |
| `all_tasks()`                  | Every task, any status, in insertion order.                                  |
| `pending()` / `in_progress()` / `completed()` / `failed_tasks()` | Per-status listings.                       |
| `summary() -> dict[str, int]`  | Count of tasks per status plus `total`.                                      |

### `Task`

A dataclass describing one queued unit of work. Fields: `id`, `name`, `priority`,
`payload`, `status` (a `TaskStatus`), `created_at`, `started_at`, `finished_at`,
`error`, `attempts`. Convenience boolean properties: `done`, `failed`, `pending`,
`in_progress`.

### `TaskStatus`

A string `Enum`: `PENDING`, `IN_PROGRESS`, `DONE`, `FAILED`.

## Development

The test suite uses only the standard library — no pytest, no installs:

```bash
python -m unittest discover -s tests -v
```

## License

[MIT](LICENSE)
