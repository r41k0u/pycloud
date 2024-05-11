from dataclasses import dataclass

import cloca
import evque

import model
import policy


@dataclass
class VmpFirstFit(policy.Vmp):
    def allocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        results = []
        for vm in vms:
            for host in self.DATACENTER.HOSTS:
                if all(host.VMM.has_capacity(vm)):
                    results.extend(host.VMM.allocate([vm]))
                    self._vm_pm[vm] = host
                    evque.publish('vm.allocate', cloca.now(), host, vm)
                    break
            else:
                results.append(False)
        return results

    def deallocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        results = []
        for vm in vms:
            host = self._vm_pm[vm]
            results.extend(host.VMM.deallocate([vm]))
            del self._vm_pm[vm]
            evque.publish('vm.deallocate', cloca.now(), host, vm)
        return results

    def stopped(self) -> list[model.Vm, ...]:
        stopped_vms = []
        for host in self.DATACENTER.HOSTS:
            # VMs in an idle state are treated as stopped, but other criteria can also be considered.
            stopped_vms.extend(host.VMM.idles())
        return stopped_vms
