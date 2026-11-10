"""Tests for desyncio.events (EVT-01)."""

import dataclasses
import types
import typing
from dataclasses import dataclass
from typing import Any, Literal

import pytest

from desyncio import events
from desyncio.events import (
    EVENT_SCHEMA_VERSION,
    Event,
    ResourceChanged,
    RunFinished,
    RunStarted,
)

# Every Event class defined in desyncio.events (Event itself included), derived
# from the module so a new built-in cannot be missed by the checks below.
BUILTINS = [
    obj
    for obj in vars(events).values()
    if isinstance(obj, type)
    and issubclass(obj, Event)
    and obj.__module__ == events.__name__
]
PRIMITIVES = {int, float, str, bool, type(None)}
SAMPLE: dict[Any, Any] = {int: 1, float: 1.0, str: "x", bool: True, type(None): None}


def _leaf_types(tp: Any) -> set[Any]:
    """Unwrap unions and Literals into the set of primitive types they allow."""
    if typing.get_origin(tp) is Literal:
        return {type(v) for v in typing.get_args(tp)}
    if typing.get_origin(tp) in (typing.Union, types.UnionType):
        return set().union(*(_leaf_types(a) for a in typing.get_args(tp)))
    return {tp}


def _build(cls: type[Event]) -> Event:
    kwargs = {
        name: SAMPLE[min(_leaf_types(tp), key=lambda t: t.__name__)]
        for name, tp in typing.get_type_hints(cls).items()
    }
    if "reason" in kwargs:
        kwargs["reason"] = "until"
    return cls(**kwargs)


def test_schema_version_and_exports() -> None:
    assert EVENT_SCHEMA_VERSION == 1
    # Both directions: every export is the version or a checked built-in, and
    # every built-in is exported.
    exported = set(events.__all__) - {"EVENT_SCHEMA_VERSION"}
    assert exported == {cls.__name__ for cls in BUILTINS}
    assert all(getattr(events, name) in BUILTINS for name in exported)


@pytest.mark.parametrize("cls", BUILTINS)
def test_builtin_events_are_primitive_frozen_slotted(cls: type[Event]) -> None:
    for name, tp in typing.get_type_hints(cls).items():
        assert _leaf_types(tp) <= PRIMITIVES, f"{cls.__name__}.{name}: {tp}"
    inst = _build(cls)
    assert not hasattr(inst, "__dict__")
    # Assigning the base field is enough: a non-frozen dataclass subclass of a
    # frozen one raises TypeError at class creation.
    with pytest.raises(dataclasses.FrozenInstanceError):
        inst.t = 9.0  # type: ignore[misc]


def test_builtin_to_dict() -> None:
    ev = ResourceChanged(t=2.5, seq=3, resource="r", in_use=1, capacity=2, queue_len=0)
    assert ev.to_dict() == {
        "t": 2.5,
        "seq": 3,
        "resource": "r",
        "in_use": 1,
        "capacity": 2,
        "queue_len": 0,
        "kind": "ResourceChanged",
    }
    assert RunStarted(t=0.0, seq=0, seed=7, until=None).to_dict()["until"] is None
    assert RunFinished(t=1.0, seq=9, reason="error").to_dict()["reason"] == "error"


def test_user_subclass() -> None:
    @dataclass(frozen=True, slots=True, kw_only=True)
    class OrderShipped(Event):
        order_id: int
        carrier: str

    ev = OrderShipped(t=1.0, seq=4, order_id=7, carrier="x")
    assert ev.to_dict() == {
        "t": 1.0,
        "seq": 4,
        "order_id": 7,
        "carrier": "x",
        "kind": "OrderShipped",
    }
    assert not hasattr(ev, "__dict__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        ev.order_id = 8  # type: ignore[misc]
