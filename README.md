# PyCloud: Modern Python Cloud simulator

PyCloud is a framework designed for modeling and simulating cloud computing environments and services. The choice of Python as the programming
language for PyCloud is for its user-friendly nature and efficient compatibility with third-party libraries. This
decision addresses certain limitations found in other simulators.

## Getting started

The simulator is designed to be easy to use and extend. To run PyCloud,

1. Clone the project

    ```bash 
    $ git clone https://github.com/r41k0u/pycloud.git 
    ```

2. Install [Python 3.10](https://wiki.python.org/moin/BeginnersGuide/Download)

3. Install dependencies
     ```bash
     $ pip install -r requirements.txt
     ```

4. Set up `PYTHONPATH` environment variable
    - Determine the path to your project source root directory (where `pycloud/src` is located).
   #### Windows PowerShell
     ```powershell
     $env:PYTHONPATH = "<path_to_project_root>;$env:PYTHONPATH"
     ```
   #### Linux (Bash)
     ```bash
     export PYTHONPATH=<path_to_project_root>:$PYTHONPATH
     ```

5. Run an example
      ```bash
      $ python3 basic_example.py
      ```

That's all. The code is minimal and easy to read. You can quickly start coding by reading the existing
code and example.

## Examples

Check out the `examples` directory. These examples demonstrate
some use cases and functionalities of the simulator.

## Topics

Here is an explanation of the main events used in the PyCloud simulator, along with a short explanation of what they mean
and when they are used:

| Name        | Description                                                                                          |
|-------------------|:-----------------------------------------------------------------------------------------------------|
| *request.arrive*  | This event happens when a request comes in. It counts and records these requests in the simulator.   |
| *request.accept*  | This happens when a request is approved, and the approval is noted down.                             |
| *request.reject*  | This occurs when a request is turned down, and the rejection is recorded.                            |
| *request.stop*    | This event is for when a request is finished or stopped, and this is also noted in the records.      |
| *action.execute*  | This deals with carrying out a series of actions. What happens depends on the specific instructions. |
| *app.start*       | This marks the beginning of an application running on a VM.                                          |
| *app.stop*        | This is when an application on a VM is stopped.                                                      |
| *container.start* | This is for starting a container on a VM.                                                            |
| *container.stop*  | This is for stopping a container on a VM.                                                            |
| *controller.start* | This indicates the start of a controller on a VM.                                                    |
| *controller.stop* | This is when a controller on a VM is stopped.                                                        |
| *deployment.run*  | This shows when a deployment is actively running.                                                    |
| *deployment.pend* | This means a deployment is waiting for resources.                                                    |
| *deployment.degrade*| This indicates a deployment is not running optimally with some replicas still pending.               |
| *deployment.scale* | This is when the size of a deployment is changed, either by adding or removing replicas.             |
| *deployment.stop* | This is when a deployment is completely stopped.                                                     |
| *vm.allocate*     | This happens when a VM is assigned to a physical machine (PM).                                       |
| *vm.deallocate*   | This is when a VM is removed or released from a PM.                                                  |
| *sim.log*         | This is for the general logging mechanism of the simulation.                                          |

The system for handling these events is made to be flexible, so developers can add new things or change it to suit their
needs.