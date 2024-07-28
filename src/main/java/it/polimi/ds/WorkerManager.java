package it.polimi.ds;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.protobuf.ByteString;

import it.polimi.ds.proto.Computation;
import it.polimi.ds.proto.ControlWorkerRequest;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
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
    private List<Computation> computations;

    private Address coordinator_address;
    private Node coordinator;
    private final long id;
    private final int group_size;

    private ServerSocketChannel data_listener = null;

    private ConcurrentMap<Long, Address> network_nodes = new ConcurrentHashMap<>();

    /**
     * Gets the task.
     *
     * @param taskId the id of the task to be obtained.
     * @return the task that has the requested id.
     */
    Task getTask(long taskId) {
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
    Computation getComputation(long groupId) {
        for (Computation computation : computations) {
            if (computation.getGroupId() == groupId) {
                return computation;
            }
        }
        return null;
    }

    public WorkerManager(String[] args) throws IOException {
        System.out.println("args: " + Arrays.toString(args));
        coordinator_address = Address.fromString(args[0]).getValue0();
        coordinator = new Node(coordinator_address);

        coordinator.send(RegisterNodeManagerRequest.newBuilder()
                .setAddress(Address.getOwnAddress().toProto())
                .setTaskSlots(TASK_SLOTS)
                .build());

        var resp = coordinator.receive(RegisterNodeManagerResponse.class);
        id = resp.getId();
        computations = resp.getComputationsList();
        group_size = resp.getGroupSize();

        // System.out.println(resp + "qui: " + id + " " + computations + " " +
        // group_size);
        data_listener = ServerSocketChannel.open();
        data_listener.bind(new InetSocketAddress(WorkerManager.DATA_PORT + (int) id));
        data_listener.configureBlocking(false);

        System.out.println("DataConnection is listeninng on "
                + Address.getOwnAddress().withPort(WorkerManager.DATA_PORT + (int) id));

        data_communicator.start();

        coordinator.send(SynchRequest.newBuilder().build());
        coordinator.receive(SynchResponse.class);

        for (ProtoTask t : resp.getTasksList()) {
            Task task = new Task(t.getId(),
                    t.getGroupId(),
                    getComputation(t.getGroupId()),
                    t.getIsCheckpoint() == 1 ? true : false,
                    group_size);

            tasks.add(task);

            new Thread(() -> {
                task.waitForData();

                /// Now the task has all the data, we can execute it
                var result = task.execute();
                System.out.println("result :" + result);

                /// TODO: successors are not passed properly, idk why
                var successors = task.getSuccessorIds();
                /// Send back to the coordinator if there is nothing more to do
                if (successors.isEmpty()) {
                    try {
                        coordinator.send(WorkerManagerRequest.newBuilder()
                                .setResult(DataResponse.newBuilder()
                                        .addAllData(result.stream()
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
                    return;
                }

                /// Otherwise send to the next in group
                successors.parallelStream().forEach(successor_id -> {
                    var successor = network_nodes.get(successor_id);
                    if (successor == null) {
                        System.out.println("ERROR: Successor not found");
                        return;
                    }

                    try {
                        Node conn = new Node(successor);
                        conn.send(DataRequest.newBuilder()
                                .setTaskId(task.getId())
                                .setSourceRole(Role.WORKER)
                                .addAllData(result.stream()
                                        .map(p -> Data.newBuilder()
                                                .setKey(p.getValue0())
                                                .setValue(p.getValue1())
                                                .build())
                                        .toList())
                                .build());
                        conn.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }).start();
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

            assert req.hasUpdateNetworkRequest();
            var network_change = req.getUpdateNetworkRequest();

            var nodes = network_change.getAddressesList();
            var ids = network_change.getTaskManagerIdsList();
            for (int i = 0; i < nodes.size(); i++) {
                network_nodes.put(ids.get(i), new Address(nodes.get(i)));
            }

            System.out.println("Network updated");

            try {
                coordinator.send(UpdateNetworkResponse.newBuilder().build());
            } catch (IOException e) {
                // UNREACHABLE, coordinator is reliable
            }

            System.out.println("network " + network_nodes);
        }
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
                        DataRequest req = conn.nonBlockReceive(DataRequest.class);

                        getTask(req.getTaskId()).addData(req);
                    }

                    iter.remove();
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
    });

    public static void main(String[] args) throws IOException {
        System.out.println("Starting WorkerManager");
        new WorkerManager(args).start();
    }
}
