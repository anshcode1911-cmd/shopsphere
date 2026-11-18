"""Unit tests for the Saga orchestrator (no cloud resources needed)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "order-service"))

from saga import SagaOrchestrator, SagaStep, StepFailed  # noqa: E402


def make(fail_at=None, infra_error=False, flaky_comp=0):
    calls, log = [], []
    flaky = {"n": flaky_comp}

    def action(name):
        def _a(ctx):
            calls.append(("do", name))
            if name == fail_at:
                if infra_error:
                    raise TimeoutError("deadline exceeded")
                raise StepFailed(f"{name} rejected")
        return _a

    def comp(name):
        def _c(ctx):
            calls.append(("undo", name))
            if flaky["n"] > 0:
                flaky["n"] -= 1
                raise ConnectionError("transient")
        return _c

    steps = [
        SagaStep("reserve_stock", action("reserve_stock"), comp("reserve_stock")),
        SagaStep("charge_payment", action("charge_payment"), comp("charge_payment")),
        SagaStep("confirm_order", action("confirm_order"), None),
    ]
    saga = SagaOrchestrator(steps, lambda *a: log.append(a))
    return saga, calls, log


def test_happy_path_runs_all_steps_without_compensation():
    saga, calls, _ = make()
    r = saga.execute({})
    assert r.succeeded
    assert r.completed_steps == ["reserve_stock", "charge_payment", "confirm_order"]
    assert not [c for c in calls if c[0] == "undo"]


def test_failure_at_first_step_compensates_nothing():
    saga, calls, _ = make(fail_at="reserve_stock")
    r = saga.execute({})
    assert not r.succeeded and r.failed_step == "reserve_stock"
    assert r.compensated_steps == []
    assert ("do", "charge_payment") not in calls


def test_payment_decline_releases_stock():
    saga, calls, log = make(fail_at="charge_payment")
    r = saga.execute({})
    assert not r.succeeded and r.failed_step == "charge_payment"
    assert r.compensated_steps == ["reserve_stock"]
    assert ("undo", "charge_payment") not in calls  # business decline -> nothing to refund
    assert ("reserve_stock", "COMPENSATE", "SUCCEEDED", "attempt 1") in log


def test_confirm_failure_compensates_in_reverse_order():
    saga, calls, _ = make(fail_at="confirm_order")
    r = saga.execute({})
    undo = [c[1] for c in calls if c[0] == "undo"]
    assert undo == ["charge_payment", "reserve_stock"]
    assert r.compensated_steps == ["charge_payment", "reserve_stock"]


def test_infra_error_also_compensates_the_uncertain_step():
    saga, calls, _ = make(fail_at="charge_payment", infra_error=True)
    r = saga.execute({})
    undo = [c[1] for c in calls if c[0] == "undo"]
    assert undo == ["charge_payment", "reserve_stock"]
    assert not r.succeeded


def test_compensation_is_retried():
    saga, calls, log = make(fail_at="charge_payment", flaky_comp=2)
    r = saga.execute({})
    assert r.compensated_steps == ["reserve_stock"]
    assert ("reserve_stock", "COMPENSATE", "SUCCEEDED", "attempt 3") in log
