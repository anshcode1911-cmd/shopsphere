"""Orchestration-based Saga (the 'concept' implemented for requirement f).

A checkout spans three services that each own their data:
    catalog-service  (Cloud SQL `catalog`)   -> reserve stock
    payment-service  (Firestore `payments`)  -> charge card
    order-service    (Cloud SQL `orders`)    -> confirm order
A distributed ACID transaction (2PC) across these stores is impossible/undesirable,
so each step is a *local* transaction paired with a *compensating* action.
If step N fails, the orchestrator runs the compensations of steps N-1..1 in
reverse order, leaving the system consistent (eventual consistency).

This module is infrastructure-free so it can be unit-tested (see tests/).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

log = logging.getLogger("order-service.saga")


class StepFailed(Exception):
    """Raised by a step action when the business operation is rejected."""


@dataclass
class SagaStep:
    name: str
    action: Callable[[dict], Any]
    compensation: Optional[Callable[[dict], Any]] = None


@dataclass
class SagaResult:
    succeeded: bool
    failed_step: Optional[str] = None
    reason: str = ""
    completed_steps: list[str] = field(default_factory=list)
    compensated_steps: list[str] = field(default_factory=list)


class SagaOrchestrator:
    """Executes steps sequentially; on failure compensates completed steps in reverse.

    `recorder(step, action, status, detail)` is called for every transition so the
    caller can persist a saga log (we store it in Cloud SQL `saga_log`).
    """

    def __init__(self, steps: list[SagaStep], recorder: Callable[[str, str, str, str], None],
                 metrics_hook: Callable[[str, str], None] | None = None,
                 compensation_retries: int = 3):
        self.steps = steps
        self.record = recorder
        self.metrics_hook = metrics_hook or (lambda step, outcome: None)
        self.compensation_retries = compensation_retries

    def execute(self, ctx: dict) -> SagaResult:
        result = SagaResult(succeeded=True)
        for step in self.steps:
            self.record(step.name, "EXECUTE", "STARTED", "")
            try:
                step.action(ctx)
            except Exception as exc:  # StepFailed (business) or infra error (timeout etc.)
                reason = str(exc) or exc.__class__.__name__
                self.record(step.name, "EXECUTE", "FAILED", reason)
                self.metrics_hook(step.name, "failed")
                result.succeeded = False
                result.failed_step = step.name
                result.reason = reason
                # A timeout/infra error leaves the remote outcome unknown (the step may have
                # been applied). Compensations are idempotent, so compensate it as well.
                to_compensate = list(result.completed_steps)
                if not isinstance(exc, StepFailed):
                    to_compensate.append(step.name)
                self._compensate(ctx, result, to_compensate)
                return result
            self.record(step.name, "EXECUTE", "SUCCEEDED", "")
            self.metrics_hook(step.name, "succeeded")
            result.completed_steps.append(step.name)
        return result

    def _compensate(self, ctx: dict, result: SagaResult, to_compensate: list[str]) -> None:
        by_name = {s.name: s for s in self.steps}
        for name in reversed(to_compensate):
            step = by_name[name]
            if step.compensation is None:
                continue
            for attempt in range(1, self.compensation_retries + 1):
                try:
                    step.compensation(ctx)
                    self.record(name, "COMPENSATE", "SUCCEEDED", f"attempt {attempt}")
                    self.metrics_hook(name, "compensated")
                    result.compensated_steps.append(name)
                    break
                except Exception as exc:  # compensations are idempotent -> safe to retry
                    self.record(name, "COMPENSATE", "FAILED", f"attempt {attempt}: {exc}")
                    log.warning("compensation %s attempt %s failed: %s", name, attempt, exc)
            else:
                self.metrics_hook(name, "compensation_failed")
