from dataclasses import dataclass, asdict
from typing import Optional

import cloca
import evque

import model
import policy
from model import Container


@dataclass
class ControlPlaneRoundRobin(policy.ControlPlane):
    def __post_init__(self):
        super().__post_init__()
        # Initializing resource dictionaries for each node.
        self._node_cpu: dict[model.Vm, float] = dict()
        self._node_ram: dict[model.Vm, int] = dict()
        self._node_gpu: dict[model.Vm, Optional[tuple[int, int] | float]] = dict()
        for node in self.CLUSTER_CONTROLLER.NODES:
            self._node_cpu[node] = node.CPU
            self._node_ram[node] = node.RAM
            self._node_gpu[node] = node.GPU

        # Initializing container and deployment related dictionaries.
        self._deployment_replicas: dict[model.Deployment, list[list[model.Container, ...], ...]] = {}
        self._container_deployment: dict[model.Container, model.Deployment] = {}
        self._container_node: dict[model.Container, model.Vm] = {}

        # List of deployments that haven't reached their desired replica count.
        # Each entry is a tuple containing the deployment and the number of replicas yet to be deployed.
        self._degraded_deployments: list[tuple[model.Deployment, int], ...] = []

        evque.subscribe('container.stop', self._delete_container)

    def _deploy_deployment(self, deployment: model.Deployment, num_replicas: int = None) -> int:
        if not num_replicas:
            num_replicas = deployment.replicas

        num_deployed_replicas = 0
        prev_num_deployed_replicas = num_deployed_replicas

        if not self._deployment_replicas.get(deployment):
            self._deployment_replicas[deployment] = []

        # Loop to continuously deploy replicas until no more can be deployed.
        while True:
            for worker in filter(lambda n: n.is_on(), self.CLUSTER_CONTROLLER.NODES):
                if num_replicas == num_deployed_replicas:
                    return num_deployed_replicas
                elif self._deploy_replica(deployment, worker):
                    num_deployed_replicas += 1

            # Terminate the loop if no new replicas were executed in this iteration.
            if prev_num_deployed_replicas == num_deployed_replicas:
                break
            prev_num_deployed_replicas = num_deployed_replicas

        return num_deployed_replicas

    def _deploy_replica(self, deployment: model.Deployment, node: model.Vm) -> bool:
        if not self._has_sufficient_resources_for_deployment(deployment, node):
            return False

        replica_containers = [Container(**container_spec) for container_spec in deployment.CONTAINER_SPECS]
        for container in replica_containers:
            self._deploy_container(container, node)
            self._container_node[container] = node
            self._container_deployment[container] = deployment

        self._deployment_replicas[deployment].append(replica_containers)
        return True

    def delete(self, deployment: model.Deployment, num_replicas: int = None) -> policy.ControlPlane:
        if not num_replicas:
            num_replicas = len(self._deployment_replicas[deployment])

        while self._deployment_replicas[deployment] and num_replicas:
            replica_containers = self._deployment_replicas[deployment].pop()
            while replica_containers:
                container = replica_containers.pop()
                self._delete_container(None, container)
            num_replicas -= 1

        if not self._deployment_replicas[deployment]:
            del self._deployment_replicas[deployment]

        return self

    def _deploy_container(self, container: model.Container, node: model.Vm) -> bool:
        if not self._has_sufficient_resources_for_container(asdict(container), node):
            return False

        # Retrieve the resources required by the container.
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(asdict(container))

        # Update the node's resources.
        self._node_cpu[node] -= requested_cpu
        self._node_ram[node] -= requested_ram
        self._node_gpu[node] = ()  # Assuming GPU resources are fully utilized and set to empty tuple

        # Schedule the container on the node.
        node.OS.schedule([container])

        return True

    def _delete_container(self, node: Optional[model.Vm], container: model.Container) -> bool:
        if not node:
            node = self._container_node[container]
        elif self._container_node[container] != node:
            raise ValueError("Container not found on the specified node.")

        # Retrieve the resources utilized by the container.
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(asdict(container))

        # Release the resources.
        self._node_cpu[node] += requested_cpu
        self._node_ram[node] += requested_ram
        self._node_gpu[node] = requested_gpu

        self._remove_container_references(container)

        return True

    def _remove_container_references(self, container: model.Container):
        deployment = self._container_deployment[container]
        del self._container_deployment[container]

        # Remove the container from node-container mapping.
        del self._container_node[container]

        # Remove the container from deployment replicas and clean up if needed.
        replicas = self._deployment_replicas.get(deployment, [])
        for i, replica in enumerate(replicas):
            if container in replica:
                replica.remove(container)

                # If the replica list is empty after removal, delete it.
                if not replica:
                    del self._deployment_replicas[deployment][i]

                    # If there are no replicas left for the deployment, delete the deployment entry.
                    if not self._deployment_replicas[deployment]:
                        del self._deployment_replicas[deployment]
                        evque.publish('deployment.stop', cloca.now(), self.CLUSTER_CONTROLLER, deployment)
                break

    def _has_sufficient_resources_for_deployment(self, deployment: model.Deployment, node: model.Vm) -> bool:
        requested_cpu, requested_ram, requested_gpu = self._get_deployment_requested_resources(deployment)
        return self._has_sufficient_resources(requested_cpu, requested_ram, requested_gpu, node)

    def _has_sufficient_resources_for_container(self, container_spec: dict, node: model.Vm) -> bool:
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(container_spec)
        return self._has_sufficient_resources(requested_cpu, requested_ram, [requested_gpu], node)

    def _has_sufficient_resources(self, requested_cpu: float, requested_ram: int,
                                  requested_gpu: list[tuple[int, int], ...], node: model.Vm) -> bool:
        has_cpu: bool = self._node_cpu[node] >= requested_cpu
        has_ram: bool = self._node_ram[node] >= requested_ram
        has_gpu: bool = not requested_gpu or self._node_gpu[node] in requested_gpu
        return has_cpu and has_ram and has_gpu

    def _get_deployment_requested_resources(self, deployment: model.Deployment) -> tuple[float, int, list[tuple[int, int], ...]]:
        total_requested_cpu, total_requested_ram, total_requested_gpu = 0, 0, []
        for container_spec in deployment.CONTAINER_SPECS:
            requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(container_spec)
            total_requested_cpu += requested_cpu
            total_requested_ram += requested_ram
            total_requested_gpu.append(requested_gpu)
        return total_requested_cpu, total_requested_ram, total_requested_gpu

    @staticmethod
    def _get_container_requested_resources(container_spec: dict) -> tuple[float, int, tuple[int, int]]:
        return container_spec['CPU'][0], container_spec['RAM'][0], container_spec['GPU']

    def _deploy_degraded_deployments(self):
        num_degraded_deployments = len(self._degraded_deployments)

        # Loop through all degraded deployments
        while num_degraded_deployments:
            deployment, num_remained_replicas = self._degraded_deployments.pop(0)
            num_remained_replicas -= self._deploy_deployment(deployment, num_remained_replicas)

            # If all required replicas were not executed, re-append to degraded deployments
            if num_remained_replicas:
                self._degraded_deployments.append((deployment, num_remained_replicas))
                evque.publish('deployment.degrade', cloca.now(), self.CLUSTER_CONTROLLER, deployment, num_remained_replicas)
            else:
                evque.publish('deployment.run', cloca.now(), self.CLUSTER_CONTROLLER, deployment)

            num_degraded_deployments -= 1

    def _deploy_pending_deployments(self):
        num_pending_deployments = len(self._pending_deployments)

        # Loop through all pending deployments
        while num_pending_deployments:
            deployment = self._pending_deployments.pop(0)
            num_deployed_replicas = self._deploy_deployment(deployment)

            # Determine the status of deployment execution
            if not num_deployed_replicas:
                self._pending_deployments.append(deployment)
                evque.publish('deployment.pend', cloca.now(), self.CLUSTER_CONTROLLER, deployment)
            elif num_deployed_replicas < deployment.replicas:
                num_remained_replicas = deployment.replicas - num_deployed_replicas
                self._degraded_deployments.append((deployment, num_remained_replicas))
                evque.publish('deployment.degrade', cloca.now(), self.CLUSTER_CONTROLLER, deployment, num_remained_replicas)
            else:
                evque.publish('deployment.run', cloca.now(), self.CLUSTER_CONTROLLER, deployment)

            num_pending_deployments -= 1

    def _deploy_scaled_deployments(self):
        num_scaled_deployments = len(self._scaled_deployments)

        # Loop through all scaled deployments
        while num_scaled_deployments:
            deployment = self._scaled_deployments.pop(0)

            current_replicas = len(self._deployment_replicas[deployment])
            required_replicas = deployment.replicas - current_replicas

            # Scale up or down based on the difference
            if required_replicas < 0:
                to_delete_replicas = abs(required_replicas)
                self.delete(deployment, to_delete_replicas)
                evque.publish('deployment.scale', cloca.now(), self.CLUSTER_CONTROLLER, deployment, required_replicas)
            elif required_replicas > 0:
                self._degraded_deployments.append((deployment, required_replicas))
                evque.publish('deployment.scale', cloca.now(), self.CLUSTER_CONTROLLER, deployment, required_replicas)
            else:
                evque.publish('deployment.run', cloca.now(), self.CLUSTER_CONTROLLER, deployment)

            num_scaled_deployments -= 1

    def manage(self):
        self._deploy_scaled_deployments()
        self._deploy_degraded_deployments()
        self._deploy_pending_deployments()

    def is_stopped(self) -> bool:
        return False


class FractionalGPUControlPlaneRoundRobin(ControlPlaneRoundRobin):
    def __post_init__(self):
        super().__post_init__()
        # Initialize GPU resources for each node in the cluster.
        for node in self.CLUSTER_CONTROLLER.NODES:
            # Set GPU availability to 1.0 (100%) if the node has a GPU, otherwise set it to 0.0.
            self._node_gpu[node] = 1.0 if node.GPU else 0.0

    def _deploy_container(self, container: model.Container, node: model.Vm) -> bool:
        # Check if the node has sufficient resources for the container.
        if not self._has_sufficient_resources_for_container(asdict(container), node):
            return False

        # Extract the resources required by the container.
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(asdict(container))

        # Deduct the resources used by the container from the node's available resources.
        self._node_cpu[node] -= requested_cpu
        self._node_ram[node] -= requested_ram
        self._node_gpu[node] -= requested_gpu

        # Schedule the container on the node.
        node.OS.schedule([container])

        return True

    def _delete_container(self, node: Optional[model.Vm], container: model.Container) -> bool:
        if not node:
            node = self._container_node[container]
        elif self._container_node[container] != node:
            raise ValueError("Container not found on the specified node.")

        # Extract the resources utilized by the container.
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(asdict(container))

        # Release the resources back to the node.
        self._node_cpu[node] += requested_cpu
        self._node_ram[node] += requested_ram
        self._node_gpu[node] += requested_gpu

        # Remove references to the container from internal data structures.
        self._remove_container_references(container)

        return True

    @staticmethod
    def _get_container_requested_resources(container_spec: dict) -> tuple[float, int, float]:
        return container_spec['CPU'][0], container_spec['RAM'][0], container_spec['GPU']

    def _has_sufficient_resources(self, requested_cpu: float, requested_ram: int, requested_gpu: float, node: model.Vm) -> bool:
        has_cpu: bool = self._node_cpu[node] >= requested_cpu
        has_ram: bool = self._node_ram[node] >= requested_ram
        has_gpu: bool = self._node_gpu[node] >= requested_gpu
        return has_cpu and has_ram and has_gpu

    def _get_deployment_requested_resources(self, deployment: model.Deployment) -> tuple[float, int, float]:
        total_requested_cpu, total_requested_ram, total_requested_gpu = 0.0, 0, 0.0
        for container_spec in deployment.CONTAINER_SPECS:
            requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(container_spec)
            total_requested_cpu += requested_cpu
            total_requested_ram += requested_ram
            total_requested_gpu += requested_gpu  # Summing up the GPU requirements for each container

        # Ensure that the total GPU requirement does not exceed 1.0 (100%)
        if total_requested_gpu > 1.0:
            AssertionError('GPU requirement must not exceed 1.0')

        return total_requested_cpu, total_requested_ram, total_requested_gpu

    def _has_sufficient_resources_for_container(self, container_spec: dict, node: model.Vm) -> bool:
        requested_cpu, requested_ram, requested_gpu = self._get_container_requested_resources(container_spec)
        return self._has_sufficient_resources(requested_cpu, requested_ram, requested_gpu, node)