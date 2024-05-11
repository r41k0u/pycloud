from dataclasses import dataclass

import cloca
import evque

import policy


@dataclass
class OsTimeShared(policy.Os):
    def resume(self, cpu: tuple[int, ...], duration: int) -> list[int, ...]:
        stopped_apps = []

        # Compute the initial cycles available for all cores
        remained_cycles = [core * duration for core in cpu]

        num_apps = len(self)
        for app in self:
            if not app.has_resumed_once():
                evque.publish(f'{type(app).__name__.lower()}.start', cloca.now(), self.VM, app)

            available_cycles = [core * duration // num_apps for core in remained_cycles]
            consumed_cycles = app.resume(available_cycles)

            # Calculate the remaining cycles after the app has consumed some
            for i in range(len(remained_cycles)):
                remained_cycles[i] -= consumed_cycles[i]

            if app.is_stopped():
                stopped_apps.append(app)

            num_apps -= 1
            if not num_apps:
                break

        # Terminate finished apps
        for stopped_app in stopped_apps:
            self.terminate([stopped_app])
            evque.publish(f'{type(app).__name__.lower()}.stop', cloca.now(), self.VM, stopped_app)

        # Return the cycles consumed on each core
        return [core * duration - rc for core, rc in zip(cpu, remained_cycles)]
