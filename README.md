# Fault-Tolerant Dataflow Platform


Author:
- Alessandro Conti: [AlessandroConti11](https://github.com/AlessandroConti11/)
- Luca Bancale: [sbancuz](https://github.com/sbancuz/)

License: [MIT license](https://github.com/AlessandroConti11/Fault-Tolerant_Dataflow_Platform/blob/master/LICENSE)

Tags: `#Apache-Flink`, `#big-data`, `#computer_engineering`, `#distributed_system`, `#fault-tollerance`, `#java`, `#map_reduce`, `#protobuf`, `#polimi`.


## University

Politecnico di Milano.

Academic Year: 2024/2025.

090950 - Distributed Systems - professor Cugola Giampaolo Saverio - optional project.


## Specification

*Specification overview:*

Implement a distributed dataflow platform for processing large amount (big-data) of key-value pairs, where keys and values are integers.
<br>
The platform includes a coordinator and multiple workers running on multiple nodes of a distributed system.
The coordinator accepts dataflow programs specified as an arbitrarily long sequence of the above operators.

*Full specification* are in the [Specification/projects_2023-2024](https://github.com/AlessandroConti11/Fault-Tolerant_Dataflow_Platform/tree/master/Specification/projects_2023-2024.pdf)


## How to run

The steps specified below are suitable for a Unix environment.

1. set environment variables in the [.env](https://github.com/AlessandroConti11/Fault-Tolerant_Dataflow_Platform/blob/master/.env) file
    - INET_IFACE
    - FAULTY_THREADS
    - FAULTY_THREADS_SECS_INTERVAL
    - FAULT_PROBABILITY
2. compile the proto message
    ```bash
   ./run proto
   ```
3. run the allocator
   - allocates the WorkerManagers
   - allocates the Coordinator
   ```bash
   ./run alloc
   ```
4. run the client
    ```bash
   ./run client ADDRESS_COORDINATOR_ALLOC OTHER_ALLOC_ADDRESSES
   ```
   Optionally, it is possible to specify the operations and files to be executed.


## Final Consideration

Final Evaluation: **4/4**