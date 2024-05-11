""" The models of the simulated environment. """

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Type, Optional, Callable

import cloca

import policy


class BaseMeta(type):
    def __init__(cls, name, bases, class_dict):
        if "__hash__" not in class_dict:
            # If the class doesn't provide a custom __hash__ method, use the one from the Base class
            cls.__hash__ = Base.__hash__


@dataclass(kw_only=True)
class Base(metaclass=BaseMeta):
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return id(self) == id(other)
        return False

    def __hash__(self):
        return hash(id(self))


@dataclass(kw_only=True)
class App(Base):
    NAME: str
    LENGTH: tuple[int, ...]
    EXPIRATION: Optional[int] = field(default=None)

    def __post_init__(self):
        # denotes initialization of app instances
        self.__has_resumed_once: bool = False
        # remained length of application threads
        self._remained: list[int, ...] = list(self.LENGTH)

    def has_resumed_once(self) -> bool:
        return self.__has_resumed_once

    def restart(self) -> App:
        self._remained = list(self.LENGTH)
        return self

    def resume(self, cpu: tuple[int, ...]) -> list[int, ...]:
        num_cores = len(cpu)
        num_threads = len(self._remained)
        remaining_cycles = list(cpu)
        consumed_cycles = [0] * num_cores

        if not self.__has_resumed_once:
            self.__has_resumed_once = True

        thread_idx = 0
        for core_idx in range(num_cores):
            while remaining_cycles[core_idx] > 0 and not self.is_stopped():
                cycles_to_spend = min(remaining_cycles[core_idx], self._remained[thread_idx])
                self._remained[thread_idx] -= cycles_to_spend
                remaining_cycles[core_idx] -= cycles_to_spend
                consumed_cycles[core_idx] += cycles_to_spend
                thread_idx = (thread_idx + 1) % num_threads

        return consumed_cycles

    def is_stopped(self) -> bool:
        # Check if the current time has surpassed the expiration time
        if self.EXPIRATION is not None and cloca.now() >= self.EXPIRATION:
            return True
        return not any(self._remained)


@dataclass(kw_only=True)
class Container(App):
    CPU: tuple[float, float]
    RAM: tuple[int, int]
    GPU: Optional[tuple[int, int] | float]


@dataclass(kw_only=True)
class Controller(App):
    NODES: list[Vm, ...]
    CONTROL_PLANE: Type[policy.ControlPlane]

    def __post_init__(self):
        super().__post_init__()
        # A worker service is scheduled on the worker nodes
        for node in self.NODES:
            node.OS.schedule([App(NAME='worker', LENGTH=self.LENGTH)])
        self.CONTROL_PLANE = self.CONTROL_PLANE(self)

    def resume(self, cpu: tuple[int, ...]) -> list[int, ...]:
        self.CONTROL_PLANE.manage()
        consumed_cycles = super().resume(cpu)
        return consumed_cycles

    def is_stopped(self) -> bool:
        return super().is_stopped() or self.CONTROL_PLANE.is_stopped()


@dataclass(kw_only=True)
class Deployment(Base):
    NAME: str
    CONTAINER_SPECS: list[dict, ...]
    replicas: int

    def __iter__(self):
        return iter(self.CONTAINER_SPECS)


@dataclass(kw_only=True)
class Vm(Base):
    NAME: str
    CPU: int
    RAM: int
    GPU: Optional[tuple[int, int]]
    OS: Type[policy.Os]

    STATE_ON = 'ON'
    STATE_OFF = 'OFF'

    state: Literal[STATE_ON, STATE_OFF] = field(init=False, default=STATE_OFF)

    def __post_init__(self):
        self.OS = self.OS(self)

    def turn_on(self) -> Vm:
        self.state = Vm.STATE_ON
        return self

    def turn_off(self) -> Vm:
        self.state = Vm.STATE_OFF
        self.OS.restart()
        return self

    def is_on(self) -> bool:
        return self.state == Vm.STATE_ON

    def is_off(self) -> bool:
        return not self.is_on()


@dataclass(kw_only=True)
class Pm(Base):
    NAME: str
    CPU: tuple[int, ...]
    RAM: int
    GPU: Optional[tuple[tuple[int, int], ...]]
    VMM: Type[policy.Vmm]

    def __post_init__(self):
        self.VMM = self.VMM(self)


@dataclass(kw_only=True)
class DataCenter(Base):
    NAME: str
    VMP: Type[policy.Vmp]
    HOSTS: list[Pm, ...] = field(default_factory=list)

    def __post_init__(self):
        self.VMP = self.VMP(self)

    def __iter__(self):
        return iter(self.HOSTS)


@dataclass(kw_only=True)
class User(Base):
    NAME: str
    REQUESTS: list[Action, ...] = field(default_factory=list)

    def __iter__(self):
        return iter(self.REQUESTS)


@dataclass(kw_only=True)
class Action(Base):
    ARRIVAL: int
    EXECUTE: Callable[[], Any]


@dataclass(kw_only=True)
class Request(Action):
    VM: Vm
    REQUIRED: bool = field(default=False)
    IGNORED: bool = field(default=False)
    EXECUTE: Optional[Callable[[], Any]] = None
    ON_SUCCESS: Optional[Callable[[], Any]] = None
    ON_FAILURE: Optional[Callable[[], Any]] = None
