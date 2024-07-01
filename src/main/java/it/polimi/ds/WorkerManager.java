package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.protobuf.ByteString;

import it.polimi.ds.proto.Computation;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.ProtoTask;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.UpdateNetworkRequest;
import it.polimi.ds.proto.UpdateNetworkResponse;
import it.polimi.ds.proto.WorkerManagerRequest;
import org.javatuples.Pair;

public class WorkerManager {

    public static final int TASK_SLOTS = 5;
    public static final int DATA_PORT = 5002;

    private Vector<Task> tasks = new Vector<>(TASK_SLOTS);
    private Vector<Thread> task_threads = new Vector<>(TASK_SLOTS);
    // Computation has a pair of group_id and the list of operations
    private List<Computation> computations;

    private Address coordinator_address;
    private Node coordinator;
    private final long id;
    private final int group_size;

    private ServerSocket data_listener = null;

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
        coordinator_address = Address.fromString(args[0]).getValue0();
        coordinator = new Node(coordinator_address);

        coordinator.send(RegisterNodeManagerRequest.newBuilder()
                .setAddress(Address.getOwnAddress().toProto())
                .setTaskSlots(TASK_SLOTS)
                .build());

        data_listener = new ServerSocket(DATA_PORT);
        data_communicator.start();

        var resp = coordinator.receive(RegisterNodeManagerResponse.class);
        id = resp.getId();
        computations = resp.getComputationsList();
        group_size = resp.getGroupSize();

        for (ProtoTask t : resp.getTasksList()) {
            Task task = new Task(t.getId(),
                    t.getGroupId(),
                    getComputation(t.getGroupId()),
                    t.getIsCheckpoint() == 1 ? true : false,
                    group_size);

            tasks.add(task);

            task_threads.add(new Thread(() -> {
                task.waitForData();

                /// Now the task has all the data, we can execute it
                var result = task.execute();

                task.getSuccessorIds().parallelStream()
                        .forEach(successor_id -> {
                            var successor = network_nodes.get(successor_id);
                            if (successor == null) {
                                System.out.println("ERROR: Successor not found");
                                return;
                            }

                            try {
                                Node conn = new Node(successor);
                                conn.send(DataRequest.newBuilder()
                                        .setTaskId(task.getId())
                                        .addAllData(result)
                                        .build());
                                conn.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            }));
        }
    }

    /// Main thread handles the connection with the coordinator that listens for
    /// changes in the network
    public void start() {
        while (true) {
            UpdateNetworkRequest network_change;
            try {
                network_change = coordinator.receive(UpdateNetworkRequest.class);
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }

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
        }
    }

    /// DataCommunicator thread handles the communication with the other nodes in
    /// the case of the initialization, the coordinator. This expects to receive the
    /// DAG information to handle the tasks
    Thread data_communicator = new Thread(() -> {
        try {
            while (true) {
                Node conn = new Node(data_listener.accept());

                try {
                    var req = conn.receive(DataRequest.class);
                    System.out.println("Received data request" + req.getDataList());
                    // TODO: Maybe put some queue to hold the data until it can receive the new data
                    // for the task

                    // TODO: Change types
                    getTask(req.getTaskId()).addData(req.getDataList());

                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        } catch (IOException e) {
            try {
                if (data_listener != null)
                    data_listener.close();
            } catch (Exception ee) {
            }

            // TODO: Close everything I guess?
            e.printStackTrace();
        }
    });

    public static void main(String[] args) throws IOException {
        System.out.println("Starting WorkerManager");
        new WorkerManager(args).start();
    }
}
