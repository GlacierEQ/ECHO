from __future__ import annotations

from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState
from echo.resource_scheduler import (
    ResourceFairScheduler,
    SchedulingIntent,
    schedule_task,
)


class Worker:
    def __init__(
        self,
        worker_id,
        capabilities,
        *,
        fitness=None,
        resources=None,
        topology=None,
        worker_version="",
        draining=False,
        inflight=0,
        max_inflight=None,
    ):
        self.worker_id = worker_id
        self.capabilities = frozenset(capabilities)
        self.fitness = fitness or {}
        self.resources = resources or {}
        self.topology = topology or {}
        self.worker_version = worker_version
        self.draining = draining
        self.inflight = inflight
        self.max_inflight = max_inflight

    async def execute(self, task, context):  # pragma: no cover - planning only
        raise AssertionError("scheduler tests must not execute workers")


def scheduled(task_id, capability, *, priority=0, **intent):
    return schedule_task(
        ExecutionTask(
            task_id,
            "run",
            required_capabilities=(capability,),
            priority=priority,
        ),
        SchedulingIntent(**intent),
    )


def test_resource_vector_places_gpu_work_only_where_capacity_exists():
    mesh = ExecutionMesh(
        [scheduled("kernel", "gpu", resources={"gpu": 1, "memory_gb": 24})]
    )
    small = Worker(
        "small",
        {"gpu"},
        fitness={"gpu": 0.99},
        resources={"gpu": 1, "memory_gb": 12},
    )
    large = Worker(
        "large",
        {"gpu"},
        fitness={"gpu": 0.8},
        resources={"gpu": 1, "memory_gb": 48},
    )
    decision = ResourceFairScheduler().plan(mesh, [small, large])
    assert [(item.task.task_id, item.backend.worker_id) for item in decision.assignments] == [
        ("kernel", "large")
    ]


def test_missing_explicit_resource_capacity_never_means_infinite_capacity():
    mesh = ExecutionMesh([scheduled("kernel", "gpu", resources={"gpu": 1})])
    opaque = Worker("opaque", {"gpu"}, fitness={"gpu": 1.0})
    decision = ResourceFairScheduler().plan(mesh, [opaque])
    assert decision.assignments == ()
    assert decision.unroutable == ("kernel",)


def test_weighted_fair_service_prevents_high_priority_flow_starvation():
    tasks = [
        scheduled("a-done", "code", fairness_key="A", priority=100),
        scheduled("a-next", "code", fairness_key="A", priority=100),
        scheduled("b-next", "code", fairness_key="B", priority=1),
    ]
    mesh = ExecutionMesh(tasks, max_concurrency=1)
    mesh.runtime["a-done"].state = TaskState.SUCCEEDED
    mesh.results["a-done"] = __import__("echo.execution_mesh", fromlist=["WorkerResult"]).WorkerResult()
    worker = Worker("code", {"code"}, fitness={"code": 1.0})

    decision = ResourceFairScheduler().plan(mesh, [worker])
    assert [item.task.task_id for item in decision.assignments] == ["b-next"]
    assert decision.fairness_service["A"] == 1.0
    assert decision.fairness_service["B"] == 0.0


def test_concurrency_seats_backpressure_expensive_task():
    mesh = ExecutionMesh(
        [scheduled("large", "code", seats=3)],
        max_concurrency=2,
    )
    worker = Worker("code", {"code"})
    decision = ResourceFairScheduler().plan(mesh, [worker])
    assert decision.assignments == ()
    assert decision.backpressured == ("large",)


def test_worker_max_inflight_is_pull_style_backpressure():
    mesh = ExecutionMesh([scheduled("a", "code")])
    saturated = Worker("sat", {"code"}, inflight=2, max_inflight=2)
    decision = ResourceFairScheduler().plan(mesh, [saturated])
    assert decision.assignments == ()
    assert decision.backpressured == ("a",)


def test_flow_backpressure_caps_one_tenant_without_blocking_another():
    mesh = ExecutionMesh(
        [
            scheduled("a-active", "code", fairness_key="A"),
            scheduled("a-wait", "code", fairness_key="A"),
            scheduled("b-wait", "code", fairness_key="B"),
        ],
        max_concurrency=3,
    )
    mesh.runtime["a-active"].state = TaskState.RUNNING
    mesh.runtime["a-active"].lease_owner = "worker"
    mesh.runtime["a-active"].lease_expires_at = 10**12
    worker = Worker("worker", {"code"})
    scheduler = ResourceFairScheduler(max_inflight_by_fairness={"A": 1})

    decision = scheduler.plan(mesh, [worker])
    assert "a-wait" in decision.backpressured
    assert "b-wait" in [item.task.task_id for item in decision.assignments]


def test_pack_keeps_group_on_same_worker():
    mesh = ExecutionMesh(
        [
            scheduled(
                "a",
                "code",
                placement_group="bundle",
                placement_strategy="pack",
            ),
            scheduled(
                "b",
                "code",
                placement_group="bundle",
                placement_strategy="pack",
            ),
        ],
        max_concurrency=2,
    )
    workers = [
        Worker("w1", {"code"}, fitness={"code": 1.0}),
        Worker("w2", {"code"}, fitness={"code": 0.9}),
    ]
    decision = ResourceFairScheduler().plan(mesh, workers)
    assert [item.backend.worker_id for item in decision.assignments] == ["w1", "w1"]


def test_spread_distributes_group_when_alternatives_exist():
    mesh = ExecutionMesh(
        [
            scheduled(
                "a",
                "code",
                placement_group="bundle",
                placement_strategy="spread",
            ),
            scheduled(
                "b",
                "code",
                placement_group="bundle",
                placement_strategy="spread",
            ),
        ],
        max_concurrency=2,
    )
    workers = [
        Worker("w1", {"code"}, fitness={"code": 1.0}),
        Worker("w2", {"code"}, fitness={"code": 0.9}),
    ]
    decision = ResourceFairScheduler().plan(mesh, workers)
    assert {item.backend.worker_id for item in decision.assignments} == {"w1", "w2"}


def test_gang_group_is_atomic_when_seats_do_not_fit():
    mesh = ExecutionMesh(
        [
            scheduled(
                "a",
                "gpu",
                seats=2,
                placement_group="gang",
                gang=True,
            ),
            scheduled(
                "b",
                "gpu",
                seats=2,
                placement_group="gang",
                gang=True,
            ),
        ],
        max_concurrency=3,
    )
    worker = Worker("gpu", {"gpu"})
    decision = ResourceFairScheduler().plan(mesh, [worker])
    assert decision.assignments == ()
    assert decision.backpressured == ("a", "b")


def test_version_topology_and_draining_are_hard_routing_boundaries():
    mesh = ExecutionMesh(
        [
            scheduled(
                "regional",
                "code",
                topology={"region": "west"},
                required_worker_version="v2",
            )
        ]
    )
    draining = Worker(
        "draining",
        {"code"},
        worker_version="v2",
        topology={"region": "west"},
        draining=True,
        fitness={"code": 2.0},
    )
    wrong_version = Worker(
        "v1",
        {"code"},
        worker_version="v1",
        topology={"region": "west"},
    )
    wrong_region = Worker(
        "east",
        {"code"},
        worker_version="v2",
        topology={"region": "east"},
    )
    correct = Worker(
        "correct",
        {"code"},
        worker_version="v2",
        topology={"region": "west"},
    )
    decision = ResourceFairScheduler().plan(
        mesh,
        [draining, wrong_version, wrong_region, correct],
    )
    assert [item.backend.worker_id for item in decision.assignments] == ["correct"]


def test_scheduling_intent_is_embedded_in_task_definition_payload():
    task = scheduled(
        "a",
        "code",
        fairness_key="tenant-a",
        fairness_weight=3,
        seats=2,
        resources={"cpu": 4},
        placement_group="p",
        placement_strategy="spread",
        topology={"zone": "z1"},
        required_worker_version="v3",
    )
    intent = SchedulingIntent.from_task(task)
    assert intent.fairness_key == "tenant-a"
    assert intent.fairness_weight == 3
    assert intent.seats == 2
    assert intent.resources == {"cpu": 4.0}
    assert task.payload["__echo_scheduling__"]["required_worker_version"] == "v3"
