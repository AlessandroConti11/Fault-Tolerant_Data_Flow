package it.polimi.ds;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.proto.ProtoComputation;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.CloseResponse;
import it.polimi.ds.proto.ControlWorkerRequest;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.FlushResponse;
import it.polimi.ds.proto.ProtoTask;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.Role;
import it.polimi.ds.proto.SynchRequest;
import it.polimi.ds.proto.SynchResponse;
import it.polimi.ds.proto.UpdateNetworkRequest;
import it.polimi.ds.proto.UpdateNetworkResponse;
import it.polimi.ds.proto.WorkerManagerRequest;
import org.javatuples.Pair;

public class WorkerManager {

    public static final int TASK_SLOTS = 5;
    public static final int DATA_PORT = 5002;

    private Vector<Task> tasks = new Vector<>(TASK_SLOTS);
    // Computation has a pair of group_id and the list of operations
    private List<ProtoComputation> computations;

    private Address coordinator_address;
    private Node coordinator;
    private final long id;
    private final int group_size;

    private ServerSocketChannel data_listener = null;

    private ConcurrentMap<Long, Address> network_nodes = new ConcurrentHashMap<>();

    // private Object flush_lock = new Object();

    /**
     * Gets the task.
     *
     * @param taskId the id of the task to be obtained.
     * @return the task that has the requested id.
     */
    Task getTask(long taskId) {
        // System.out.println("Ask for task " + taskId + " that have " +
        // tasks.stream().map(Task::getId).toList());
        for (Task task : tasks) {
            if (task.getId() == taskId) {
                return task;
            }
        }
        return null;
    }

    /**
     * Gets the computation.
     *
     * @param groupId the id of the group to get the computation of.
     * @return the computation of the group requested.
     */
    ProtoComputation getComputation(long groupId) {
        for (var computation : computations) {
            if (computation.getGroupId() == groupId) {
                return computation;
            }
        }
        return null;
    }

    public WorkerManager(String[] args) throws IOException {
        final int PID = Integer.parseInt(args[1]);
        final long ALLOCATOR_ID = Long.parseLong(args[2]);

        // System.out.println("args: " + Arrays.toString(args));
        coordinator_address = Address.fromString(args[0]).getValue0();
        coordinator = new Node(coordinator_address);

        coordinator.send(RegisterNodeManagerRequest.newBuilder()
                .setAddress(Address.getOwnAddress().withPort(PID + WorkerManager.DATA_PORT).toProto())
                .setTaskSlots(TASK_SLOTS)
                .setAllocatorId(ALLOCATOR_ID)
                .build());

        var resp = coordinator.receive(RegisterNodeManagerResponse.class);
        id = resp.getId();

        // DON'T COMMENT OUT THIS LINE
        System.out.println(Allocator.WM_MESSAGE_PREFIX + id);

        computations = resp.getComputationsList();
        group_size = resp.getGroupSize();

        data_listener = ServerSocketChannel.open();
        data_listener.bind(new InetSocketAddress(PID + WorkerManager.DATA_PORT));
        data_listener.configureBlocking(false);

        System.out.println("DataConnection is listening on "
                + Address.getOwnAddress().withPort(PID + WorkerManager.DATA_PORT));

        data_communicator.start();

        for (ProtoTask t : resp.getTasksList()) {
            Task task = new Task(t.getId(),
                    t.getGroupId(),
                    getComputation(t.getGroupId()),
                    t.getIsCheckpoint() == 1 ? true : false,
                    group_size);

            tasks.add(task);

            new Thread(() -> {
                while (true) {
                    processTask(task);
                }
            }).start();
        }

        coordinator.send(SynchRequest.newBuilder().build());
        coordinator.receive(SynchResponse.class);
    }

    private Map<Long /* group_id */, Integer /* counter */> group_checkpoint_map = new ConcurrentHashMap<>();

    public synchronized boolean canSendCheckpointRequest(Task task) {
        /// This is not form a checkpoint recovery request
        if (!group_checkpoint_map.containsKey(task.getId())) {
            return task.isCheckpoint();
        }

        int cont = group_checkpoint_map.get(task.getGroupId());
        group_checkpoint_map.compute(task.getGroupId(), (k, v) -> v == 1 ? null : v - 1);

        return cont == group_size;
    }

    public void processTask(Task task) {
        task.waitForData();

        System.out.println("Processing task id: " + task.getId());
        FaultyThread.maybeCrash();
        final Map<Long, List<Long>> successors;
        final List<DataRequest.Builder> requests;

        synchronized (task) {
            if (!task.hasAlreadyComputed()) {
                /// Now the task has all the data, we can execute it
                System.out.println("Begin execution of task " + task.getId());
                task.execute();
                System.out.println("Finish execution of task " + task.getId());
            }

            /// TODO: successors are not passed properly, idk why. <- Maybe outdated
            successors = task.getSuccessorMap();
            /// Send back to the coordinator if there is nothing more to do
            if (successors.isEmpty()) {
                writeBackResult(coordinator, task);
                return;
            }

            /// Otherwise send to the next in group
            requests = task.getSuccessorsDataRequests();
            /// Assert that we have only one group as a successors
            assert successors.entrySet().stream().map(e -> e.getValue().size()).reduce(0,
                    (sum, val) -> sum + val) == group_size
                    : "Aggregated number of successors doesn't match group size: " + group_size;

            if (canSendCheckpointRequest(task)) {
                try {
                    writeBackCheckpoint(coordinator, task);

                    /// Wait for the coordinator to tell this worker to go on with the computation.
                    /// This notice is in the form of a flush request that tells that the next group
                    /// that it's ready to compute
                    System.out.println("TASK WAIT " + task.getId());
                    task.wait();
                    System.out.println("---------------------TASK NON WAIT");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

        successors.keySet().parallelStream().forEach(successor_id -> {
            var successor = network_nodes.get(successor_id);
            if (successor == null) {
                System.out.println("ERROR: Successor not found");
                return;
            }

            System.out.println("Sending results to " + successor_id);

            while (true) {
                try {
                    Node conn = new Node(successor);
                    System.out.println("task: " + task.getId() + " next: " +
                            successors.get(successor_id));

                    successors.get(successor_id).stream().forEach(next_task_id -> {
                        assert task.getId() < next_task_id : "Task id is in successors group";

                        var req = requests.get((int) (next_task_id % task.getGroupSize()))
                                .setSourceRole(Role.WORKER)
                                .setSourceTask(task.getId())
                                .setComputationId(task.getComputationId())
                                .setTaskId(next_task_id).build();
                        try {
                            conn.send(req);
                        } catch (IOException e) {
                            System.out.println(
                                    "Crashed on sending" + task.getId() + " ------------------- " + e.getMessage());
                        }
                    });
                    conn.close();

                    break;
                } catch (IOException e) {
                    System.out.println("A successor is not available");
                    synchronized (network_nodes) {
                        try {
                            network_nodes.wait();
                        } catch (InterruptedException e1) {
                            // e1.printStackTrace();
                        }
                    }
                }
            }
        });

    }

    public void writeBackCheckpoint(Node node, Task task) {
        System.out.println("Sending back checkpoint");
        var req = CheckpointRequest.newBuilder()
                .setComputationId(task.getComputationId())
                .setSourceTaskId(task.getId());

        if (!task.skipCheckpointWriteBack()) {
            req.addAllData(task.getResult().stream()
                    .map(p -> Data.newBuilder()
                            .setKey(p.getValue0())
                            .setValue(p.getValue1())
                            .build())
                    .toList());
        } else {
            req.setIsFromAnotherCheckpoint(1);
        }

        try {
            node.send(WorkerManagerRequest.newBuilder()
                    .setCheckpointRequest(
                            req.build())
                    .build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeBackResult(Node node, Task task) {
        System.out.println("Sending back results");
        try {
            node.send(WorkerManagerRequest.newBuilder()
                    .setResult(DataResponse.newBuilder()
                            .setComputationId(task.getComputationId())
                            .setSourceTask(task.getId())
                            .addAllData(task.getResult().stream()
                                    .map(p -> Data.newBuilder()
                                            .setKey(p.getValue0())
                                            .setValue(p.getValue1())
                                            .build())
                                    .toList())
                            .build())
                    .build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /// Main thread handles the connection with the coordinator that listens for
    /// changes in the network
    public void start() {
        while (true) {
            ControlWorkerRequest req;
            try {
                req = coordinator.receive(ControlWorkerRequest.class);
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }

            if (req.hasCloseRequest()) {
                System.out.println("Bye bye");
                System.exit(0);
            }

            if (req.hasFlushRequest()) {
                var f_req = req.getFlushRequest();

                List<Task> tt = tasks.stream()
                        .filter(t -> f_req.getGroupsIdList().contains(t.getGroupId()))
                        .toList();

                tt.forEach(t -> t.flushComputation((long) f_req.getComputationId()));

                try {
                    coordinator.send(WorkerManagerRequest.newBuilder()
                            .setFlushResponse(FlushResponse.newBuilder()
                                    .setComputationId(f_req.getComputationId())
                                    .build())
                            .build());

                } catch (IOException e) {
                    e.printStackTrace();
                }

                continue;
            }

            if (req.hasFlushOk()) {
                System.out.println("FLUSH OK " + req.getFlushOk().getComputationId());
                var f_req = req.getFlushOk();

                List<Task> tt = tasks.stream()
                        .filter(t -> f_req.getGroupsIdList().contains(t.getGroupId()))
                        .toList();

                /// Notify the other thread that the computation can now resume since the next
                /// stage of computation is free
                tt.forEach(t -> {
                    synchronized (t) {
                        t.notifyAll();
                    }
                });

                continue;
            }

            assert req.hasUpdateNetworkRequest() : "Forgot to add ControlWorkerRequest to handle: " + req;
            synchronized (network_nodes) {
                var network_change = req.getUpdateNetworkRequest();

                var nodes = network_change.getAddressesList();
                var ids = network_change.getTaskManagerIdsList();
                if (ids.size() != nodes.size())
                    continue;

                for (int i = 0; i < nodes.size(); i++) {
                    network_nodes.put(ids.get(i), new Address(nodes.get(i)));
                }

                // System.out.println("Network updated");
                try {
                    coordinator.send(UpdateNetworkResponse.newBuilder().build());
                } catch (IOException e) {
                    // UNREACHABLE, coordinator is reliable
                }

                /// If a successor crashes, the other thread will wait, so we just unlock it
                network_nodes.notifyAll();
            }

            // System.out.println("network " + network_nodes);
        }

        System.out.println("Bye bye");
        System.exit(0);
    }

    /// DataCommunicator thread handles the communication with the other nodes in
    /// the case of the initialization, the coordinator. This expects to receive the
    /// DAG information to handle the tasks
    Thread data_communicator = new Thread(() -> {
        try {
            Selector sel = Selector.open();
            data_listener.register(sel, SelectionKey.OP_ACCEPT);

            while (true) {
                sel.select();

                var iter = sel.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    if (key.isAcceptable()) {
                        SocketChannel conn = data_listener.accept();
                        conn.configureBlocking(false);
                        conn.register(sel, SelectionKey.OP_READ);
                    }

                    if (key.isReadable()) {
                        Node conn = new Node(((SocketChannel) key.channel()).socket());
                        try {
                            DataRequest req = conn.nonBlockReceive(DataRequest.class);
                            assert req.getSourceRole() == Role.MANAGER || req.getSourceRole() == Role.WORKER
                                    : "Got message from unexpected source";

                            Task t = getTask(req.getTaskId());
                            assert t != null : "Unknown task id expected: " + tasks + " got: " + req.getTaskId();

                            if (!req.hasCrashedGroup()) {
                                t.addData(req);
                            } else {
                                group_checkpoint_map.putIfAbsent(t.getGroupId(), getGroupSize());

                                t.restartFromCheckpoint(req);
                            }
                        } catch (ClosedChannelException e) {
                            conn.close();
                        }
                    }

                    iter.remove();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    });

    public int getGroupSize() {
        return group_size;
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting WorkerManager");
        new FaultyThread().start();
        new WorkerManager(args).start();
    }
}
