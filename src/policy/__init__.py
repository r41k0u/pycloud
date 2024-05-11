""" The algorithms within the simulated environment. """

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import model


@dataclass
class Os(ABC):
    VM: model.Vm

    def __post_init__(self):
        # list of applications assigned to the operating system for execution
        self._running_apps: list[model.App, ...] = []
        # list of terminated apps
        self._stopped_apps: list[model.App, ...] = []

    def __contains__(self, app: model.App) -> bool:
        return app in self._running_apps

    def __iter__(self):
        return iter(self._running_apps)

    def __len__(self) -> int:
        return len(self._running_apps)

    def __contains__(self, app: model.App) -> bool:
        return app in self._running_apps or app in self._stopped_apps

    def schedule(self, apps: list[model.App, ...]) -> list[bool, ...]:
        self._running_apps.extend(apps)
        return [True] * len(apps)

    def terminate(self, apps: list[model.App, ...]) -> Os:
        for app in apps:
            self._running_apps.remove(app)
            self._stopped_apps.append(app)
        return self

    def restart(self) -> Os:
        self._running_apps.clear()
        self._stopped_apps.clear()
        return self

    @abstractmethod
    def resume(self, cpu: tuple[int, ...], duration: int) -> list[int, ...]:
        pass

    def stopped(self) -> list[model.App, ...]:
        finished_apps = self._stopped_apps
        self._stopped_apps = []
        return finished_apps

    def is_idle(self) -> bool:
        return not bool(self._running_apps)


@dataclass
class Vmm(ABC):
    HOST: model.Pm

    def __post_init__(self):
        # the list of allocated VMs
        self._guests: list[model.Vm, ...] = []

    def __contain__(self, vm: model.Vm) -> bool:
        return vm in self._guests

    def __iter__(self):
        return iter(self._guests)

    def __len__(self) -> int:
        return len(self._guests)

    @abstractmethod
    def has_capacity(self, vm: model.Vm) -> bool:
        pass

    @abstractmethod
    def allocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        pass

    @abstractmethod
    def deallocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        pass

    @abstractmethod
    def resume(self, duration: int) -> Vmm:
        pass

    def idles(self) -> list[model.Vm, ...]:
        return [guest for guest in self._guests if guest.OS.is_idle()]


@dataclass
class Vmp(ABC):
    DATACENTER: model.DataCenter

    def __post_init__(self):
        # An internal mapping from VM instances to their respective nodes (PM).
        self._vm_pm: dict[model.Vm, model.Pm] = {}

    def __getitem__(self, vm: model.Vm) -> model.Pm:
        return self._vm_pm[vm]

    def empty(self) -> bool:
        return not bool(self._vm_pm)

    @abstractmethod
    def allocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        pass

    def resume(self, duration: int) -> Vmp:
        for host in self.DATACENTER.HOSTS:
            host.VMM.resume(duration)
        return self

    @abstractmethod
    def deallocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        pass

    @abstractmethod
    def stopped(self) -> list[model.Vm, ...]:
        pass


@dataclass
class ControlPlane(ABC):
    CLUSTER_CONTROLLER: model.Controller

    def __post_init__(self):
        # Deployments submitted for execution
        self._pending_deployments: list[model.Deployment] = []
        # Deployments with a scaling request
        self._scaled_deployments: list[model.Deployment] = []

    def apply(self, deployment: model.Deployment) -> ControlPlane:
        self._pending_deployments.append(deployment)
        return self

    def scale(self, deployment: model.Deployment, replicas: int) -> ControlPlane:
        deployment.replicas = replicas
        self._scaled_deployments.append(deployment)
        return self

    @abstractmethod
    def delete(self, deployment: model.Deployment, num_replicas: int = None) -> ControlPlane:
        pass

    @abstractmethod
    def is_stopped(self) -> bool:
        pass

    @abstractmethod
    def manage(self):
        pass
