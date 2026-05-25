# agent-task-queue

Priority queue for sub-tasks in agent loops. Zero dependencies.

```python
from agent_task_queue import TaskQueue

q = TaskQueue()
q.enqueue("search web", priority=2, payload={"query": "cats"})
q.enqueue("summarize results", priority=1)

while not q.empty:
    task = q.dequeue()
    try:
        result = run(task)
        q.complete(task.id)
    except Exception as e:
        q.fail(task.id, str(e))
```

## Install

```bash
pip install agent-task-queue
```
