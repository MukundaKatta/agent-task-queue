"""token-budget - shared token + dollar budget for concurrent LLM tasks.

Fan-out workloads (agents, batch evals, parallel summarizers) race many
tasks to consume from one budget. This library is a tiny thread-safe and
asyncio-friendly counter with two axes (tokens, USD) that returns
`BudgetExceeded` when a record would push past a configured cap.

    from token_budget import BudgetPool, BudgetExceeded

    pool = BudgetPool(token_cap=1_000_000, usd_cap=10.0)
    try:
        pool.record(tokens=1200, usd=0.0036)
    except BudgetExceeded as e:
        # tell the worker to skip this call
        ...

Use `try_reserve` / `release` for two-phase commit if you want to atomic-
reserve before doing the LLM call and refund on failure:

    with pool.reserve(tokens=2000, usd=0.012) as reservation:
        result = call_llm(...)
        reservation.commit(tokens=result.actual_tokens, usd=result.actual_cost)

Both axes are optional - pass only `token_cap` or only `usd_cap` and the
other axis is unbounded.

Sibling to the Rust crate `token-budget-pool`.
"""

from token_budget.pool import (
    BudgetExceeded,
    BudgetPool,
    BudgetSnapshot,
    Reservation,
)

__version__ = "0.1.0"

__all__ = [
    "BudgetExceeded",
    "BudgetPool",
    "BudgetSnapshot",
    "Reservation",
    "__version__",
]
