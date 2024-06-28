package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.protobuf.ByteString;

import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.UpdateNetworkRequest;
import it.polimi.ds.proto.UpdateNetworkResponse;
import it.polimi.ds.proto.WorkerManagerRequest;

public class WorkerManager {

    public static final int TASK_SLOTS = 5;
    public static final int DATA_PORT = 5002;

    private Vector<Task> tasks = new Vector<>(TASK_SLOTS);
    private Address coordinator_address;
    private Node coordinator;
    private final long id;

    private ServerSocket data_listener = null;

    private ConcurrentMap<Long, Address> network_nodes = new ConcurrentHashMap<>();

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
        for (long task : resp.getTaskIdsList()) {
            tasks.add(new Task(task, false));
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
                    System.out.println("Received data request" + req.getData());
                    // TODO: Handle the data, get the task and execute it, if it's possible

                    conn.send(DataResponse.newBuilder()
                            .setData(ByteString.copyFromUtf8("Data received"))
                            .build());
                    System.out.println("Sent data response");
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

    class Task {
        private long id;
        private boolean is_checkpoint;
        private volatile boolean has_all_data = false;
        private Object data;

        public Task(long id, boolean is_checkpoint) {
            this.id = id;
            this.is_checkpoint = is_checkpoint;
        }

        public boolean isReady() {
            return has_all_data;
        }

        public void execute() {
            System.out.println("Executing task " + id);
        }
    }
}
