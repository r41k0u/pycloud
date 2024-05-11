from dataclasses import dataclass

import model
import policy


@dataclass
class VmmSpaceShared(policy.Vmm):
    def __post_init__(self):
        super().__post_init__()
        self._free_cpu: set[model.Vm, ...] = {core for core in range(len(self.HOST.CPU))}
        self._vm_cpu: dict[model.Vm, set[int, ...]] = {}
        self._free_ram: int = self.HOST.RAM
        self._free_gpu: tuple[set[int], ...] = tuple({block for block in range(blocks)} for _, blocks in self.HOST.GPU)
        self._vm_gpu: dict[model.Vm, tuple[int, set[int, ...]]] = {}

    def has_capacity(self, vm: model.Vm) -> tuple[bool, bool, bool]:
        has_cpu_capacity = len(self._free_cpu) >= vm.CPU
        has_ram_capacity = self._free_ram >= vm.RAM
        has_gpu_capacity = not vm.GPU or any(self.find_gpu_blocks(vm.GPU, gpu) for gpu in self._free_gpu)

        return has_cpu_capacity, has_ram_capacity, has_gpu_capacity

    def allocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        results = []
        for vm in vms:
            # Check if there is enough overall capacity (CPU, RAM, GPU) for the VM
            if not all(self.has_capacity(vm)):
                results.append(False)
                continue
            self._vm_cpu[vm] = {self._free_cpu.pop() for core in range(vm.CPU)}
            self._free_ram -= vm.RAM
            if vm.GPU:
                for gpu_idx, free_gpu in enumerate(self._free_gpu):
                    if all_gpu_blocks := self.find_gpu_blocks(vm.GPU, free_gpu):
                        gpu_blocks = all_gpu_blocks.pop(0)
                        free_gpu.difference_update(gpu_blocks)
                        self._vm_gpu[vm] = gpu_idx, gpu_blocks
                        break
            self._guests.append(vm)
            results.append(True)
            vm.turn_on()
        return results

    def deallocate(self, vms: list[model.Vm, ...]) -> list[bool, ...]:
        results = []
        for vm in vms:
            if vm not in self:
                results.append(False)
                continue
            self._free_cpu.update(self._vm_cpu[vm])
            del self._vm_cpu[vm]
            self._free_ram += vm.RAM
            if vm.GPU:
                gpu, blocks = self._vm_gpu[vm]
                self._free_gpu[gpu].update(blocks)
                del self._vm_gpu[vm]
            self._guests.remove(vm)
            results.append(True)
            vm.turn_off()
        return results

    def resume(self, duration: int) -> policy.Vmm:
        for vm in self:
            if vm.is_on():
                vm_cpu = [self.HOST.CPU[core] for core in self._vm_cpu[vm]]
                vm.OS.resume(vm_cpu, duration)
        return self

    def find_gpu_blocks(self, profile: tuple[int, int], gpu: set[int, ...]) -> list[set[int], ...]:
        result = []
        _, num_memory_blocks = profile
        for start in gpu:
            blocks = set(range(start, start + num_memory_blocks))
            if blocks.issubset(gpu):
                result.append(blocks)
        return result
