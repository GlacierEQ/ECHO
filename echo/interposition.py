"""Composable execution-boundary interposition for ECHO workers.

Interposition lets policy, evidence, tracing, caching, normalization, and other
cross-cutting execution mechanics wrap any WorkerBackend without coupling those
mechanics to the worker implementation itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol, Sequence

from echo.execution_mesh import (
    ExecutionContext,
    ExecutionTask,
    WorkerBackend,
    WorkerResult,
)


class ExecutionInterceptor(Protocol):
    async def before(
        self,
        task: ExecutionTask,
        context: ExecutionContext,
    ) -> ExecutionContext: ...

    async def after(
        self,
        task: ExecutionTask,
        context: ExecutionContext,
        result: WorkerResult,
    ) -> WorkerResult: ...


class InterposedWorker:
    """Wrap a worker in an onion-style execution middleware chain.

    ``before`` hooks run in declaration order. ``after`` hooks run in reverse
    order, matching nested middleware semantics. Raising from a before hook
    prevents compute from executing at all.
    """

    def __init__(
        self,
        backend: WorkerBackend,
        interceptors: Sequence[ExecutionInterceptor],
    ) -> None:
        self.backend = backend
        self.interceptors = tuple(interceptors)
        self.worker_id = backend.worker_id
        self.capabilities = backend.capabilities
        if hasattr(backend, "fitness"):
            self.fitness = getattr(backend, "fitness")

    async def execute(
        self,
        task: ExecutionTask,
        context: ExecutionContext,
    ) -> WorkerResult:
        current = context
        entered: list[ExecutionInterceptor] = []
        for interceptor in self.interceptors:
            current = await interceptor.before(task, current)
            entered.append(interceptor)
        result = await self.backend.execute(task, current)
        for interceptor in reversed(entered):
            result = await interceptor.after(task, current, result)
        return result


BeforeHook = Callable[
    [ExecutionTask, ExecutionContext],
    Awaitable[ExecutionContext],
]
AfterHook = Callable[
    [ExecutionTask, ExecutionContext, WorkerResult],
    Awaitable[WorkerResult],
]


@dataclass(frozen=True)
class FunctionalInterceptor:
    """Small adapter for composing async hook functions into a middleware chain."""

    before_hook: BeforeHook
    after_hook: AfterHook

    async def before(
        self,
        task: ExecutionTask,
        context: ExecutionContext,
    ) -> ExecutionContext:
        return await self.before_hook(task, context)

    async def after(
        self,
        task: ExecutionTask,
        context: ExecutionContext,
        result: WorkerResult,
    ) -> WorkerResult:
        return await self.after_hook(task, context, result)
