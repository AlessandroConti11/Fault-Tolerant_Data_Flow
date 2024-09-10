package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.Directed_Acyclic_Graph.ManageDAG;
import it.polimi.ds.function.Function;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.ClientRequest;
import it.polimi.ds.proto.CloseRequest;
import it.polimi.ds.proto.CloseResponse;
import it.polimi.ds.proto.ProtoComputation;
import it.polimi.ds.proto.ControlWorkerRequest;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.FlushRequest;
import it.polimi.ds.proto.FlushResponse;
import it.polimi.ds.proto.ManagerTaskMap;
import it.polimi.ds.proto.NodeManagerInfo;
import it.polimi.ds.proto.Operation;
import it.polimi.ds.proto.ProtoTask;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.ReturnCode;
import it.polimi.ds.proto.Role;
import it.polimi.ds.proto.SynchRequest;
import it.polimi.ds.proto.SynchResponse;
import it.polimi.ds.proto.UpdateNetworkRequest;
import it.polimi.ds.proto.UpdateNetworkResponse;
import it.polimi.ds.proto.WorkerManagerRequest;
import org.javatuples.Pair;

public class Coordinator {

    /// This is set in the main
    private static int PID;

    private static int CLIENT_PORT = 15000;
    private static int WORKER_PORT = 25000;

    private final ExecutorService exe = Executors.newCachedThreadPool();

    private ConcurrentMap<Long, WorkerManagerHandler> workers = new ConcurrentHashMap<>();
    private ByteString program;
    private List<Address> allocators;

    private ConcurrentMap<Long, ResultBuilder> result_builders = new ConcurrentHashMap<>();

    private ManageDAG dag = null;
    private AtomicLong just_connected = new AtomicLong(0);

    private Map<Long, Integer> flushing_comp = new ConcurrentHashMap<>();
    private volatile boolean closing = false;

    public Coordinator() {
    }

    public void start() {
        clientListener.start();

        /// Stall the main thread
        Object lock = new Object();
        try {
            synchronized (lock) {
                lock.wait();
            }
        } catch (Exception e) {
        }
    }

    Thread workerListener = new Thread(() -> {
        try {
            // System.out.println(PID + " " + WORKER_PORT);
            ServerSocket workerListener = new ServerSocket(PID + WORKER_PORT);
            while (true) {
                Node node = new Node(workerListener.accept());

                just_connected.addAndGet(1);
                synchronized (just_connected) {
                    just_connected.notifyAll();
                }
                var wm_handler = new WorkerManagerHandler(node);
                exe.submit(wm_handler);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    Thread heartbeat = new Thread(() -> {
        Object lock = new Object();
        ExecutorService hb_executor = Executors.newSingleThreadExecutor();
        Future<Boolean> alloc_future = null;
        try {
            List<Long> crashed = new Vector<>();
            List<Long> allocated = new Vector<>();

            while (!closing) {
                synchronized (lock) {
                    for (var worker : workers.entrySet()) {
                        boolean alive = worker.getValue().isAlive();
                        if (!alive) {
                            dag.getComputationsOfWM(worker.getKey()).stream().forEach(c -> {
                                if (flushing_comp.containsKey(c)) {
                                    flushing_comp.compute(c, (k, v) -> (v == 1) ? null : v - 1);
                                }
                            });
                            dag.addFreeTaskManager((int) (long) worker.getKey());
                            workers.remove(worker.getKey());
                            crashed.add(worker.getKey());

                        }
                    }
                }

                if (crashed.size() > 0) {
                    System.out.println("Trying to allocate " + crashed.size() + " resources");

                    crashed.parallelStream().forEach(crashed_id -> {
                        try {
                            allocateResources(dag, 1, crashed_id);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

                    System.out.println("Allocated");
                    /// Wait until all the workers are ready, then, once we have collected all the
                    /// network information, notify the WorkerManagers of all the others' addresses
                    workers.forEach((k, v) -> {
                        v.notifyNetworkChange();
                    });

                    allocated.addAll(crashed);
                    crashed.clear();
                }

                if (allocated.size() > 0) {
                    if (alloc_future == null) {
                        alloc_future = hb_executor.submit(() -> {
                            waitAllocatedWorkers(dag.getNumberOfTaskManager());
                            System.out.println("Start heartbeat");

                            synchronized (workers) {
                                /// Let go of all the locks that are waiting for the network to be ready
                                workers.notifyAll();
                            }
                            return true;
                        });
                    } else if (alloc_future != null && alloc_future.state() == Future.State.RUNNING) {
                        int ready = workers.entrySet().stream().map(e -> e.getValue())
                                .mapToInt(e -> e.isReady() ? 1 : 0)
                                .sum();
                        System.out.println("Still waiting " + dag.getNumberOfTaskManager() + " got " + ready + " size: "
                                + workers.size());

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    } else if (alloc_future.isDone()) {
                        var comp_list = allocated.stream().map(tm_id -> {
                            List<Long> impacted_groups = dag.getGroupsOfTaskManager(tm_id);
                            return impacted_groups.stream()
                                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                                    .flatMap(Optional::stream)
                                    .distinct()
                                    .collect(Collectors.toList());
                        }).flatMap(List::stream).collect(Collectors.toSet());

                        if (comp_list.size() == 0) {
                            System.out.println("No running computation impacted");
                            // + dag.getGroupsOfTaskManager(tm_id) + " " + dag.getComputations());
                            continue;
                        }

                        System.out.println("Sending checkpoint");
                        for (var comp_id : comp_list) {
                            long grp = dag.getGroupOfLastCheckpoint(comp_id);
                            if (grp == -1) {
                                var data = dag.getDataRequestsForGroup(comp_id, 0);
                                sendComputation(data, 0, comp_id, grp);
                            } else {
                                sendComputation(dag.getLastCheckpoint(comp_id), grp, comp_id, grp);
                            }
                        }
                        allocated.clear();
                        alloc_future = null;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Shutting down heartbeat");
    });

    Thread clientListener = new Thread(() -> {
        try {
            /// Wait for a client to connect
            ServerSocket client_listener = new ServerSocket(PID + CLIENT_PORT);

            /// WARNING: This has to be the first thing that the coordinator prints
            /// This sends the address back to the host process
            System.out.println("ADDRESS::" + Address.getOwnAddress().withPort(PID + CLIENT_PORT));
            Node client = new Node(client_listener.accept());

            /// Receive from the client the allocation request that will contian the number
            /// of Wokermanagers needed for the computation, the address of the allocators
            /// and the program to be executed
            var allocation_request = client.receive(AllocationRequest.class);

            /// Create the schedule of the program using a DAG
            program = allocation_request.getRawProgram();
            try {
                dag = new ManageDAG(program, allocation_request.getNumberOfAllocations(),
                        allocation_request.getAllocatorsCount());
            } catch (Exceptions.MalformedProgramFormatException e) {
                client.send(AllocationResponse.newBuilder()
                        .setCode(ReturnCode.INVALID_PROGRAM)
                        .build());

                client.close();
                System.exit(0);
            } catch (Exceptions.NotEnoughResourcesException e) {
                client.send(AllocationResponse.newBuilder()
                        .setCode(ReturnCode.NOT_ENOUGH_RESOURCES)
                        .build());

                client.close();
                System.exit(0);
            }

            /// Allocate the WorkerManagers on the appropriate allocators
            allocators = allocation_request.getAllocatorsList().stream().map(Address::new)
                    .collect(Collectors.toList());

            workerListener.start();
            heartbeat.start();

            allocateResources(dag, dag.getNumberOfTaskManager());
            /// Wait until all the workers are ready, then, once we have collected all the
            /// network information, notify the WorkerManagers of all the others' addresses
            workers.forEach((k, v) -> {
                v.notifyNetworkChange();
            });

            System.out.println("Allocated resources");
            waitAllocatedWorkers(dag.getNumberOfTaskManager());
            System.out.println("Workers ready");

            /// Send back the OK to the client, this will signal that the network is ready
            client.send(AllocationResponse.newBuilder()
                    .build());

            // System.out.println("Client Network ready");

            while (true) {
                try {
                    var req = client.receive(ClientRequest.class);
                    if (req.hasDataRequest()) {
                        System.out.println("Received data request ");
                        exe.submit(() -> {
                            var data_req = req.getDataRequest();
                            assert data_req.getSourceRole() == Role.CLIENT
                                    : "Computation request somehow didn't come from the client";
                            long comp_id = dag.newComputation(data_req.getDataList());

                            result_builders.put(comp_id, new ResultBuilder(comp_id, dag.getMaxTasksPerGroup()));
                            var data = dag.getDataRequestsForGroup(comp_id, 0);

                            System.out.println("Sending over computation " + comp_id);
                            // computation workers + last reduce
                            startComputation(data, 0, comp_id);

                            try {
                                client.send(result_builders.get(comp_id).waitForResult());
                            } catch (IOException e) {
                                /// Unreachable since client is reliable
                                e.printStackTrace();
                            }

                            System.out.println("COMPUTATION FINISHED");

                            var grps = dag.getStageFromTask(dag.getNumberOfTask() - 1);

                            /// Send the flushing request to the appropriate worker manager
                            var wm_sets = grps.stream().map(g -> dag.getManagersOfGroup(g))
                                    .collect(Collectors.toList());
                            Set<Long> wm_ids = new HashSet<>();
                            for (Set<Long> wms : wm_sets) {
                                wm_ids.addAll(wms);
                            }

                            wm_ids.forEach(wm -> {
                                var w = workers.get(wm);
                                w.flush(comp_id, grps);
                            });

                            dag.finishComputation(comp_id);
                        });
                    } else if (req.hasCloseRequest()) {
                        System.out.println("Closing connection");

                        /// Close the hearthbeat thread
                        closing = true;
                        heartbeat.join();

                        workers.values().parallelStream().forEach(w -> {
                            try {
                                w.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });

                        client.send(CloseResponse.newBuilder().build());
                        System.exit(0);
                    }
                } catch (Exception e) {
                    System.out.println("Client disconnected -- " + e.getMessage());
                    break;
                }
            }

            client.close();

        } catch (

        Exception e) {
            e.printStackTrace();
        }
    });

    public void startComputation(List<DataRequest.Builder> data, long group_id, long comp_id) {
        // final var managers = dag.getManagersOfGroup(group_id);
        // for (var wm_id : managers) {
        sendComputation(data, group_id, comp_id, -1);
        // }
    }

    public void sendComputation(List<DataRequest.Builder> data, long group_id, long comp_id, long checkpoint) {
        assert data.size() == dag.getMaxTasksPerGroup() : "Tried to send the wrong type of request";
        dag.getTasksOfGroup(group_id).parallelStream()
                // .filter(t -> dag.getTaskInTaskManager(wm_id).contains(t))
                .forEach(t -> {
                    try {
                        var req = data.get((int) (long) t % dag.getMaxTasksPerGroup())
                                .setComputationId(comp_id)
                                .setSourceRole(Role.MANAGER)
                                .setTaskId(t)
                                .setSourceTask(-1);

                        if (checkpoint != -1) {
                            req.setCrashedGroup(checkpoint);
                        }

                        try {
                            workers.get(dag.getManagerOfTask((long) t).get())
                                    .send(req.build());
                        } catch (Exception ee) {
                            synchronized (workers) {
                                try {
                                    workers.wait();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            var w = workers.get(dag.getManagerOfTask((long) t).get());
                            assert w != null : "workers: " + workers.keySet() + " manager: "
                                    + dag.getManagerOfTask((long) t).get() + "task: " + t;

                            w.send(req.build());
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    class ResultBuilder {
        private DataResponse.Builder resp_aggregator = DataResponse.newBuilder();
        private final int max_data_count;
        private Object lock = new Object();
        private Vector<Long> fragments = new Vector<>();

        public ResultBuilder(long comp_id, int max_data_count) {
            this.max_data_count = max_data_count;
            resp_aggregator.setComputationId(comp_id);
        }

        public synchronized void addData(DataResponse r) {
            assert resp_aggregator.getComputationId() == r.getComputationId()
                    : "Getting results for a different computation " + r.getComputationId() + " want: "
                            + resp_aggregator.getComputationId();

            /// TODO: assert that this comes from a repeated computation somehow
            if (fragments.contains(r.getSourceTask())) {
                return;
            }

            assert fragments.size() < max_data_count : "A computation is still going after it has finished";

            resp_aggregator.addAllData(r.getDataList());
            fragments.add(r.getSourceTask());
            // System.out.println("fragments : " + fragments + " max : " + max_data_count);
            if (fragments.size() >= max_data_count) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }

        public DataResponse waitForResult() {
            System.out.println("Waiting for results");
            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // The position of the last operation to compute
            int lastReduce = dag.getOperationsGroup().size();

            // The last reduce - if needed
            if (dag.getOperationsGroup().get(lastReduce - 1).get(0).getValue0().equals(OperatorName.REDUCE)) {
                // The final data computed from the worker.
                List<Pair<Integer, Integer>> finalData = ManageCSVfile.readCSVinput(resp_aggregator.getDataList());

                // Compute the last reduce.
                Operator operator = new Operator();
                finalData = operator.operations(dag.getOperationsGroup().get(lastReduce - 1), finalData);

                // The data after the reduce
                DataResponse.Builder dataResponse = DataResponse.newBuilder();
                dataResponse
                        .setComputationId(resp_aggregator.getComputationId())
                        .setSourceTask(resp_aggregator.getSourceTask())
                        .addAllData(finalData.stream()
                                .map(p -> Data.newBuilder()
                                        .setKey(p.getValue0())
                                        .setValue(p.getValue1())
                                        .build())
                                .toList())
                        .build();

                // System.out.println("Sending back results for " + dataResponse.getDataList());
                System.out.println("Sending back results");
                return dataResponse.build();
            }

            // System.out.println("Sending back results for " +
            // resp_aggregator.getComputationId());
            System.out.println("Sending back results");
            return resp_aggregator.build();
        }
    }

    class WorkerManagerHandler implements Runnable {
        private Node control_connection;
        private Node data_connection;

        public static final int CHECKPOINT_TIMEOUT = 1000;
        private volatile boolean network_changed = true;
        private volatile boolean alive = false;

        private final boolean is_last;
        private final boolean is_checkpoint;

        private long id;
        private Address address;

        public WorkerManagerHandler(Node conn) throws IOException {
            this.control_connection = conn;

            /// TODO: Handle this case, in theory it should never happen, but you never
            /// know. This happens when we initialize to many workerManagers somehow
            var registration = conn.receive(RegisterNodeManagerRequest.class);
            this.id = dag.getNextFreeTaskManager(registration.getAllocatorId()).orElseThrow();

            this.address = new Address(registration.getAddress());

            List<Long> tasks = dag.getTasksOfTaskManager((int) id);
            this.is_last = tasks.stream()
                    .map(t -> dag.groupFromTask((long) t).get())
                    .anyMatch(g -> dag.isLastGroup(g));

            this.is_checkpoint = tasks.stream()
                    .map(t -> dag.groupFromTask((long) t).get())
                    .anyMatch(g -> dag.isCheckpoint(g));

            var operations = dag.getOperationsForTaskManager(id);

            // System.out.println("max task" + dag.getMaxTasksPerGroup());
            // System.out.println("ID " + id);
            conn.send(RegisterNodeManagerResponse.newBuilder()
                    .setId(id)
                    .addAllTasks(tasks.stream()
                            .map(t -> ProtoTask.newBuilder()
                                    .setId(t)
                                    .setGroupId(dag.groupFromTask((long) t).get())
                                    .setIsCheckpoint(dag.isCheckpoint(dag.groupFromTask((long) t).get()) ? 1 : 0)
                                    .build())
                            .collect(Collectors.toList()))
                    .setGroupSize(dag.getMaxTasksPerGroup())
                    .addAllComputations(operations.stream()
                            .map(op -> ProtoComputation.newBuilder()
                                    .setGroupId(op.getValue1())
                                    .addAllManagersMapping(
                                            /// Get next group
                                            dag.getManagersOfGroup((long) op.getValue1() + 1).stream()
                                                    .map(m_id -> ManagerTaskMap.newBuilder()
                                                            .setManagerSuccessorId(m_id)
                                                            .addAllTaskId(dag.getTaskInTaskManager(m_id).stream()
                                                                    .filter(t_id -> dag
                                                                            .getTasksOfGroup((long) op.getValue1() + 1)
                                                                            .contains(t_id))
                                                                    .toList())
                                                            .build())
                                                    .collect(Collectors.toList()))
                                    .addAllOperations(op.getValue0().stream()
                                            .map(o -> Operation.newBuilder()
                                                    .setOperatorName(o.getValue0().ordinal())
                                                    .setFunctionName(o.getValue1().ordinal())
                                                    .setInput(o.getValue2())
                                                    .build())
                                            .collect(Collectors.toList()))
                                    .build())
                            .collect(Collectors.toList()))
                    .build());

            /// Don't start the data connection until we have received the signal from the
            /// WorkerManager. Once we receive the signal we know that the WorkerManager is
            /// ready to receive data and it's listening on port DATA_PORT + id
            conn.receive(SynchRequest.class);
            conn.send(SynchResponse.newBuilder().build());

            // System.out.println("Data connection with " + id + " opened on "
            // + address);

            data_connection = new Node(address);

            alive = true;
            workers.put(this.id, this);
        }

        public boolean isAlive() {
            return alive;
        }

        public boolean isReady() {
            return alive && !network_changed;
        }

        public void notifyNetworkChange() {
            network_changed = true;
        }

        public void send(DataRequest req) throws IOException {
            data_connection.send(req);
            System.out.println("Sent data request for task " + req.getTaskId());
        }

        public void close() throws IOException {
            control_connection.send(ControlWorkerRequest
                    .newBuilder()
                    .setCloseRequest(CloseRequest
                            .newBuilder()
                            .build())
                    .build());

            control_connection.close();
            data_connection.close();
        }

        public void flush(Long comp_id, List<Long> group_ids) {
            try {
                control_connection.send(ControlWorkerRequest
                        .newBuilder()
                        .setFlushRequest(FlushRequest.newBuilder()
                                .setComputationId(comp_id)
                                .addAllGroupsId(group_ids)
                                .build())
                        .build());

                flushing_comp.compute(comp_id, (k, v) -> (v == null) ? 1 : v + 1);
                // flushin_comp.add(comp_id);
            } catch (IOException e) {
                System.out.println(
                        "Failed to send over a flush request, probably a workerManager crashed -- "
                                + e.getMessage());
            }
        }

        @Override
        public void run() {
            Object lock = new Object();
            // System.out.println("Worker manager connected");

            try {
                while (alive) {
                    System.out.println("-----Control thread " + id);
                    try {
                        var req = control_connection.receive(WorkerManagerRequest.class, CHECKPOINT_TIMEOUT);
                        if (req.hasCheckpointRequest()) {
                            var checkpoint = req.getCheckpointRequest();
                            assert is_checkpoint : "Got a writeback form a non-checkpoint manager";
                            assert dag.groupFromTask(checkpoint.getSourceTaskId()).get() < dag
                                    .getNumberOfGroups() : "Checkpoint doesn't make sense to be the last group";

                            exe.submit(() -> {
                                synchronized (lock) {
                                    try {
                                        boolean is_checkpoint_complete = dag.saveCheckpoint(checkpoint);
                                        if (is_checkpoint_complete) {
                                            var c_req = req.getCheckpointRequest();
                                            long comp_id = c_req.getComputationId();
                                            long src_task_id = c_req.getSourceTaskId();

                                            System.out.println("Flushing computation " + comp_id);
                                            var grps = dag.getStageFromTask(src_task_id);

                                            /// If it's not the result of a checkpoint recovery, then wait for the next
                                            /// step to be empty
                                            if (c_req.getIsFromAnotherCheckpoint() == 0) {
                                                dag.waitForNextStageToBeFree(src_task_id);
                                                dag.moveForwardWithComputation(comp_id);
                                            }

                                            /// Send the flushing request to the appropriate worker manager
                                            var wm_sets = grps.stream().map(g -> dag.getManagersOfGroup(g))
                                                    .collect(Collectors.toList());
                                            Set<Long> wm_ids = new HashSet<>();
                                            for (Set<Long> wms : wm_sets) {
                                                wm_ids.addAll(wms);
                                            }

                                            wm_ids.forEach(wm -> {
                                                var w = workers.get(wm);
                                                w.flush(comp_id, grps);
                                            });
                                            System.out.println("Flushing complete " + comp_id);
                                        }
                                    } catch (Throwable t) {
                                        t.printStackTrace();
                                    }
                                }
                            });
                        } else if (req.hasFlushResponse()) {
                            var f_resp = req.getFlushResponse();
                            assert flushing_comp.containsKey(f_resp.getComputationId())
                                    : "Tried to flush a computation that doesn't need it somehow";
                            System.out.println("Received flush response");

                            System.out.println("FLUSH ---- " + flushing_comp.get(f_resp.getComputationId()));
                            flushing_comp.compute(f_resp.getComputationId(), (k, v) -> (v == 1) ? null : v - 1);
                            System.out.println("AFTER FLUSHH ---- " + f_resp.getComputationId() + " " + flushing_comp);
                            if (flushing_comp.containsKey(f_resp.getComputationId())) {
                                dag.releaseLocks(f_resp.getComputationId());
                            }
                        } else if (req.hasResult()) {
                            assert is_last : "Got a write back from a non-last manager";

                            exe.submit(() -> {
                                try {
                                    result_builders.get(req.getResult().getComputationId()).addData(req.getResult());
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                            });
                        } else {
                            assert false : "Forgot to add the handling case for a new message " + req;
                        }
                    } catch (SocketTimeoutException e) {
                        if (network_changed) {
                            System.out.println("Sending update network request to " + id);
                            control_connection.send(
                                    ControlWorkerRequest
                                            .newBuilder()
                                            .setUpdateNetworkRequest(UpdateNetworkRequest.newBuilder()
                                                    .addAllTaskManagerIds(workers.keySet())
                                                    .addAllAddresses(workers.values().stream()
                                                            .map(w -> w.address.toProto())
                                                            .collect(Collectors.toList()))
                                                    .build())
                                            .build());

                            var ok = control_connection.receive(UpdateNetworkResponse.class);
                            network_changed = false;
                        }
                    } catch (IOException e) {
                        alive = false;
                    }
                }

            } catch (Throwable e) {
                System.out.println("Exception escaped");
                e.printStackTrace();
            }
            alive = false;
        }

    }

    private void allocateResources(ManageDAG dag, int requested_workers, Address allocator) throws IOException {
        assert requested_workers > 0;

        Node h = new Node(allocator);

        h.send(AllocateNodeManagerRequest.newBuilder()
                .setNodeManagerInfo(NodeManagerInfo.newBuilder()
                        .setAddress(Address.getOwnAddress().withPort(PID + WORKER_PORT).toProto())
                        .setNumContainers(requested_workers).build())
                .build());

        var resp = h.receive(AllocateNodeManagerResponse.class);
        while (just_connected.getAcquire() < requested_workers) {
            try {
                synchronized (just_connected) {
                    just_connected.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        just_connected.addAndGet(-requested_workers);
    }

    private void allocateResources(ManageDAG dag, int requested_workers, long wm_id) throws IOException {
        final int index = (int) ((allocators.size() / dag.getNumberOfTaskManager()) * wm_id);

        allocateResources(dag, requested_workers, allocators.get(allocators.size() - 1 - index));
    }

    private void allocateResources(ManageDAG dag, int requested_workers) throws IOException {
        final int flat_requested_number = dag.getNumberOfTaskManager() / allocators.size();
        final int remainder = dag.getNumberOfTaskManager() % allocators.size();

        for (int i = 0; i < allocators.size(); i++) {
            final int divided_requested_workers = flat_requested_number
                    + (i < remainder ? 1 : 0);

            allocateResources(dag, divided_requested_workers, allocators.get(i));
        }
    }

    void waitAllocatedWorkers(int requestedWorkers) {
        int ready = workers.values().stream()
                .mapToInt(e -> e.isReady() ? 1 : 0)
                .sum();

        while (ready < requestedWorkers) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ready = workers.values().stream()
                    .mapToInt(e -> e.isReady() ? 1 : 0)
                    .sum();
        }
    }

    public static void main(String[] args) throws SocketException {
        PID = Integer.parseInt(args[0]) % 60_000;

        Coordinator coord = new Coordinator();
        coord.start();
    }
}
