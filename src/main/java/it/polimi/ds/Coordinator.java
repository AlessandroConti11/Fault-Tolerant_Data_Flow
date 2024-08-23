package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

public class Coordinator {

    public static int CLIENT_PORT = 5000;
    public static int WORKER_PORT = 5001;

    private final ExecutorService exe = Executors.newCachedThreadPool();

    private ConcurrentMap<Long, WorkerManagerHandler> workers = new ConcurrentHashMap<>();
    private ByteString program; // TODO: Change this into the actual type after parsing step
    private List<Address> allocators;

    private ConcurrentMap<Long, ResultBuilder> result_builders = new ConcurrentHashMap<>();

    private ManageDAG dag = null;
    private AtomicLong just_connected = new AtomicLong(0);

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

    public void startWorker() {
        workerListener.start();
        heartbeat.start();
    }

    void allocNodeManagers(Node conn) throws IOException {
    }

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
                        boolean alive = worker.getValue().checkAlive();
                        if (!alive) {
                            dag.addFreeTaskManager((int) (long) worker.getKey());
                            workers.remove(worker.getKey());
                            crashed.add(worker.getKey());
                        }
                    }
                }

                if (crashed.size() > 0) {
                    System.out.println("Trying to allocate " + crashed.size());
                    allocateResources(crashed.size());
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
                            System.out.println("Network ready");

                            synchronized (workers) {
                                /// Let go of all the locks that are waiting for the network to be ready
                                workers.notifyAll();
                            }
                            return true;
                        });
                    } else if (alloc_future != null && alloc_future.state() == Future.State.RUNNING) {
                        int ready = workers.entrySet().stream().map(e -> e.getValue())
                                .mapToInt(e -> e.isReady() ? 1 : 0).sum();
                        System.out.println("Still waiting " + dag.getNumberOfTaskManager() + " got " + ready + " size: "
                                + workers.size());

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    } else if (alloc_future.isDone()) {
                        for (long tm_id : allocated) {
                            List<Long> impacted_groups = dag.getGroupsOfTaskManager(tm_id);
                            var comp_list = impacted_groups.stream()
                                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                                    .flatMap(Optional::stream)
                                    .distinct()
                                    .collect(Collectors.toList());

                            assert comp_list.size() <= 1 : "Somehow a crash impacted more than one computation";
                            if (comp_list.size() == 0) {
                                /// TODO: Still a bug here
                                System.out.println("No running computation impacted "
                                        + dag.getGroupsOfTaskManager(tm_id) + " " + dag.getComputations());
                                continue;
                            }

                            var comp_id = comp_list.get(0);

                            System.out.println("Sending checkpoint");
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
            ServerSocket client_listener = new ServerSocket(CLIENT_PORT);
            Node client = new Node(client_listener.accept());

            /// Receive from the client the allocation request that will contian the number
            /// of Wokermanagers needed for the computation, the address of the allocators
            /// and the program to be executed
            var allocation_request = client.receive(AllocationRequest.class);

            /// Create the schedule of the program using a DAG
            program = allocation_request.getRawProgram();
            try {
                dag = new ManageDAG(program, allocation_request.getNumberOfAllocations());
            } catch (Exceptions.MalformedProgramFormatException e) {
                client.send(AllocationResponse.newBuilder()
                        .setCode(ReturnCode.INVALID_PROGRAM)
                        .build());

                client.close();
                System.exit(0);
            } catch (Exceptions.NotEnoughResourcesException e) {
                client.send(AllocationResponse.newBuilder()
                        .setCode(ReturnCode.INVALID_PROGRAM)
                        .build());

                client.close();
                System.exit(0);
            }

            System.out.println("workers: " + workers.size());

            startWorker();

            /// Allocate the WokerManagers on the appropriate allocators
            allocators = allocation_request.getAllocatorsList().stream().map(a -> new Address(a))
                    .collect(Collectors.toList());

            allocateResources(dag.getNumberOfTaskManager());
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

            System.out.println("Network ready");

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
                            sendComputation(data, 0, comp_id);

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

    public void sendComputation(List<DataRequest.Builder> data, long group_id, long comp_id) {
        sendComputation(data, group_id, comp_id, -1);
    }

    public void sendComputation(List<DataRequest.Builder> data, long group_id, long comp_id, long checkpoint) {
        assert data.size() == dag.getMaxTasksPerGroup() : "Tried to send the wrong type of request";
        dag.getTasksOfGroup(group_id).parallelStream().forEach(t -> {
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

    Thread workerListener = new Thread(() -> {
        try {
            ServerSocket workerListener = new ServerSocket(WORKER_PORT);
            while (true) {
                Node node = new Node(workerListener.accept());
                /// TODO: Handle this case, in theory it should never happen, but you never know.
                /// This happens when we initialize to many workerManagers somehow
                long id = dag.getNextFreeTaskManager().orElseThrow();

                just_connected.addAndGet(1);
                synchronized (just_connected) {
                    just_connected.notifyAll();
                }
                workers.put(id, new WorkerManagerHandler(node, id));
                exe.submit(workers.get(id));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

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

            assert fragments.size() < max_data_count : "A computation is still going after it has finished";

            /// TODO: assert that this comes from a repeated computation somehow
            if (fragments.contains(r.getSourceTask())) {
                return;
            }

            resp_aggregator.addAllData(r.getDataList());
            fragments.add(r.getSourceTask());
            System.out.println("fragments : " + fragments + " max : " + max_data_count);
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

            /// TODO: MAKE LAST REDUCE

            System.out.println("Sending back results for " + resp_aggregator.getComputationId());
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

        public WorkerManagerHandler(Node conn, long id) throws IOException {
            this.control_connection = conn;
            this.id = id;

            var registration = conn.receive(RegisterNodeManagerRequest.class);
            this.address = new Address(registration.getAddress()).withPort(WorkerManager.DATA_PORT + (int) (long) id);

            List<Long> tasks = dag.getTasksOfTaskManager((int) id);
            this.is_last = tasks.stream()
                    .map(t -> dag.groupFromTask((long) t).get())
                    .anyMatch(g -> dag.isLastGroup(g));

            this.is_checkpoint = tasks.stream()
                    .map(t -> dag.groupFromTask((long) t).get())
                    .anyMatch(g -> dag.isCheckpoint(g));

            var operations = dag.getOperationsForTaskManager(id);

            System.out.println("max task" + dag.getMaxTasksPerGroup());
            System.out.println("ID " + id);
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

            System.out.println("Data connection with " + id + " opened on "
                    + address);

            data_connection = new Node(address);

            alive = true;
        }

        public boolean checkAlive() {
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

                flushin_comp.add(comp_id);
            } catch (IOException e) {
                System.out.println(
                        "Failed to send over a flush request, probably a workerManager crashed -- "
                                + e.getMessage());
            }
        }

        private Vector<Long> flushin_comp = new Vector<>();

        @Override
        public void run() {
            System.out.println("Worker manager connected");

            try {
                while (alive) {
                    System.out.println("-----Contorl thread " + id);
                    try {
                        var req = control_connection.receive(WorkerManagerRequest.class, CHECKPOINT_TIMEOUT);
                        if (req.hasCheckpointRequest()) {
                            var checkpoint = req.getCheckpointRequest();
                            assert is_checkpoint : "Got a writeback form a non-checkpoint manager";
                            assert dag.groupFromTask(checkpoint.getSourceTaskId()).get() < dag
                                    .getNumberOfGroups() : "Checkpoint doesn't make sense to be the last group";

                            exe.submit(() -> {
                                boolean is_checkpoint_complete = dag.saveCheckpoint(checkpoint);
                                if (is_checkpoint_complete) {
                                    var c_req = req.getCheckpointRequest();
                                    long comp_id = c_req.getComputationId();
                                    long src_task_id = c_req.getSourceTaskId();

                                    System.out.println("Flushing comp " + comp_id);
                                    var grps = dag.getStageFromTask(src_task_id);
                                    dag.waitForNextStageToBeFree(src_task_id);
                                    dag.moveForwardWithComputation(comp_id);

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
                                }
                            });
                        } else if (req.hasFlushResponse()) {
                            var f_resp = req.getFlushResponse();
                            assert flushin_comp.contains(f_resp.getComputationId())
                                    : "Tried to flush a computation that doesn't need it somehow";
                            System.out.println("Received flush response");

                            flushin_comp.remove(f_resp.getComputationId());
                            dag.releaseLocks(f_resp.getComputationId());
                        } else if (req.hasResult()) {
                            assert is_last : "Got a writeback from a non-last manager";

                            exe.submit(() -> {
                                try {
                                    result_builders.get(req.getResult().getComputationId()).addData(req.getResult());
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                            });
                        } else {
                            assert false : "Forgot to add the handling case for a new message";
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

    void allocateResources(int requestedWorkers) throws IOException {
        Node h = new Node(allocators.get(0));
        h.send(AllocateNodeManagerRequest.newBuilder()
                .setNodeManagerInfo(NodeManagerInfo.newBuilder()
                        .setAddress(Address.getOwnAddress().withPort(WORKER_PORT).toProto())
                        .setNumContainers(requestedWorkers).build())
                .build());

        var resp = h.receive(AllocateNodeManagerResponse.class);
        while (just_connected.getAcquire() < requestedWorkers) {
            try {
                synchronized (just_connected) {
                    just_connected.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        just_connected.addAndGet(-requestedWorkers);
    }

    void waitAllocatedWorkers(int requestedWorkers) {
        int ready = workers.entrySet().stream()
                .map(e -> e.getValue())
                .mapToInt(e -> e.isReady() ? 1 : 0)
                .sum();

        while (ready < requestedWorkers) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ready = workers.entrySet().stream()
                    .map(e -> e.getValue())
                    .mapToInt(e -> e.isReady() ? 1 : 0)
                    .sum();
        }
    }

    public static void main(String[] args) throws SocketException {
        /// This sends the address back to the host process
        System.out.println("ADDRESS::" + Address.getOwnAddress().withPort(CLIENT_PORT));

        Coordinator coord = new Coordinator();
        coord.start();
    }
}
