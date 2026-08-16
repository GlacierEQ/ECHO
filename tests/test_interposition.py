from __future__ import annotations

import asyncio

import pytest

from echo.execution_mesh import ExecutionContext, ExecutionTask, WorkerResult
from echo.interposition import FunctionalInterceptor, InterposedWorker


class Worker:
    worker_id = "base"
    capabilities = frozenset({"code"})
    fitness = {"code": 0.9}

    def __init__(self):
        self.calls = 0

    async def execute(self, task, context):
        self.calls += 1
        return WorkerResult(
            output={"trace": ["worker"], "workspace": context.workspace_id},
            terminal={"ok": True},
            agent_steps=1,
            tool_calls=1,
        )


def context():
    return ExecutionContext(
        workspace_id="ws",
        dependency_outputs={},
        dependency_terminals={},
        attempt=1,
    )


def test_interposition_wraps_worker_in_onion_order():
    events = []

    def middleware(name):
        async def before(task, ctx):
            events.append(f"before:{name}")
            return ctx

        async def after(task, ctx, result):
            events.append(f"after:{name}")
            return WorkerResult(
                output={**result.output, name: True},
                stream=result.stream,
                terminal=result.terminal,
                agent_steps=result.agent_steps,
                tool_calls=result.tool_calls,
            )

        return FunctionalInterceptor(before, after)

    base = Worker()
    wrapped = InterposedWorker(base, [middleware("outer"), middleware("inner")])
    result = asyncio.run(wrapped.execute(ExecutionTask("a", "run"), context()))

    assert events == [
        "before:outer",
        "before:inner",
        "after:inner",
        "after:outer",
    ]
    assert result.output["outer"] is True
    assert result.output["inner"] is True
    assert wrapped.fitness == {"code": 0.9}


def test_blocking_preflight_prevents_compute_execution():
    async def deny(task, ctx):
        raise PermissionError("preflight denied")

    async def passthrough(task, ctx, result):
        return result

    base = Worker()
    wrapped = InterposedWorker(
        base,
        [FunctionalInterceptor(deny, passthrough)],
    )

    with pytest.raises(PermissionError, match="preflight denied"):
        asyncio.run(wrapped.execute(ExecutionTask("a", "run"), context()))
    assert base.calls == 0
