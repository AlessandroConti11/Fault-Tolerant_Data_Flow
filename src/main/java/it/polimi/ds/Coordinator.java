package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.Directed_Acyclic_Graph.ManageDAG;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.ClientRequest;
import it.polimi.ds.proto.CloseResponse;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.NodeManagerInfo;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.WorkerManagerRequest;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Coordinator {

    public static int CLIENT_PORT = 5000;
    public static int WORKER_PORT = 5001;

    private ConcurrentMap<Long, WorkerManagerHandler> workers = new ConcurrentHashMap<>();
    private ByteString program; // TODO: Change this into the actual type after parsing step
    private List<Address> allocators;

    /**
     * Manage the direct acyclic graph.
     */
    private ManageDAG dag = new ManageDAG();

    private volatile long worker_managers_counter = 0;

    private Object response_lock = new Object();
    private DataResponse response = null;

    public Coordinator() {
    }

    public void start() {
        clientListener.start();
        workerListener.start();
        heartbeat.start();

        // TODO: This is just temporary, we should have to wait until the computation is
        // done
        try {
            clientListener.join();
            workerListener.join();
            heartbeat.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void allocNodeManagers(Node conn) throws IOException {
        var allocation_request = conn.receive(AllocationRequest.class);
        dag.setNumberOfTask(allocation_request.getNumberOfTasks());

        allocators = allocation_request.getAllocatorsList().stream().map(a -> new Address(a))
                .collect(Collectors.toList());

        Node h = new Node(allocators.get(0));
        h.send(AllocateNodeManagerRequest.newBuilder()
                .setNodeManagerInfo(NodeManagerInfo.newBuilder()
                        .setAddress(Address.getOwnAddress().withPort(WORKER_PORT).toProto())
                        .setNumContainers(allocation_request.getNumberOfTasks()).build())
                .build());

        var resp = h.receive(AllocateNodeManagerResponse.class);

        conn.send(AllocationResponse.newBuilder()
                .build());
    }

    Thread clientListener = new Thread(() -> {
        try {
            ServerSocket client_listener = new ServerSocket(CLIENT_PORT);
            Node client = new Node(client_listener.accept());

            allocNodeManagers(client);
            waitUntilAllWorkersAreReady(dag.getNumberOfTask());

            while (true) {
                try {
                    var req = client.receive(ClientRequest.class);
                    if (req.hasDataRequest()) {
                        System.out.println("Received data request " + req.getDataRequest().getData().toStringUtf8());

                        //save all operation
                        List<Triplet<OperatorName, FunctionName, Integer>> operations = ManageCSVfile.readCSVoperation(req.getOperationRequest());
                        //save all data
                        List<Pair<Integer, Integer>> data = ManageCSVfile.readCSVinput(req.getDataRequest().getData());
                        dag.setData(data);

                        //divide operation into subgroups ending with a Change Key & define the number of operation group needed.
                        dag.generateOperationsGroup(operations);

                        //divide the task in group & assign the group order
                        dag.divideTaskInGroup();

                        //TODO how to decide how many checkpoints are needed
                        //assign the checkpoint
                        dag.assignCheckpoint(2);

                        //Set of groups of checkpoint.
                        HashSet<Integer> checkpoint = dag.getCheckPoints();
                        //Map groupID and Tasks in the group.
                        HashMap<Integer, HashSet<Integer>> groupTask = dag.getTasksInGroup();
                        //Map the actual group and the follower.
                        HashMap<Integer, Integer> dagFollowerGroupGroup = dag.getFollowerGroup();


                        //get the tasks that are checkpoint and inform them
                        for (Integer groupID : checkpoint) {
                            //Tasks in the group.
                            HashSet<Integer> tasks = groupTask.get(groupID);

                            for (Integer taskID : tasks) {
                                //TODO send a message to all tasks within a checkpoint that are checkpoints
                            }
                        }

                        //get the tasks and inform them about their follow group
                        for (Integer groupID : groupTask.keySet()) {
                            //Task in the group.
                            HashSet<Integer> tasks = groupTask.get(groupID);

                            for (Integer taskID : tasks) {
                                //Follower group
                                Integer followerGroup = dagFollowerGroupGroup.get(groupID);

                                //TODO send a message to all task in the group which group is the follower - followerGroup
                            }
                        }

                        //TODO fix it - send only the operations to be computed by each individual group
                        var worker = workers.get(0L);
                        worker.send(req.getDataRequest());
                        System.out.println("Sent data request");

                        client.send(waitForDataResponse());
                    } else if (req.hasCloseRequest()) {
                        System.out.println("Closing connection");
                        client.send(CloseResponse.newBuilder().build());
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Client disconnected -- " + e.getMessage());
                    break;
                }
            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    Thread workerListener = new Thread(() -> {
        ExecutorService executors = Executors.newCachedThreadPool();

        try {
            ServerSocket workerListener = new ServerSocket(WORKER_PORT);
            while (true) {
                long id = worker_managers_counter++;
                workers.put(id, new WorkerManagerHandler(new Node(workerListener.accept())));
                executors.submit(workers.get(id));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    Thread heartbeat = new Thread(() -> {
        Object lock = new Object();
        try {
            while (true) {
                boolean networkChanged = false;
                for (var worker : workers.entrySet()) {
                    synchronized (lock) {
                        boolean alive = worker.getValue().checkAlive();
                        if (!alive) {
                            workers.remove(worker.getKey());
                            networkChanged = true;
                        }
                    }

                }

                if (networkChanged) {
                    for (var worker : workers.entrySet()) {
                        worker.getValue().notifyNetworkChange();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    class WorkerManagerHandler implements Runnable {
        private Node control_connection;
        private Node data_connection;

        public static final int CHECKPOINT_TIMEOUT = 1000;
        private volatile boolean network_changed = false;
        private volatile boolean alive = true;

        private final boolean is_last = true;

        public WorkerManagerHandler(Node conn) throws IOException {
            this.control_connection = conn;

            var registration = conn.receive(RegisterNodeManagerRequest.class);
            conn.send(RegisterNodeManagerResponse.newBuilder().setId(0).build());
            data_connection = new Node(new Address(registration.getAddress()).withPort(WorkerManager.DATA_PORT));
        }

        public boolean checkAlive() {
            return alive;
        }

        public void notifyNetworkChange() {
            network_changed = true;
        }

        /// TODO: This has to be changed, it's just placeholder
        public void send(DataRequest req) throws IOException {
            System.out.println("Sending data request");
            data_connection.send(req);

            if (is_last) {
                new Thread(() -> {
                    try {
                        response = data_connection.receive(DataResponse.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Received response");

                    synchronized (response_lock) {
                        response_lock.notifyAll();
                    }
                }).start();
            }
        }

        @Override
        public void run() {
            System.out.println("Worker manager connected");
            try {
                while (true) {
                    try {
                        var req = control_connection.receive(CheckpointRequest.class, CHECKPOINT_TIMEOUT);
                        // TODO: Do something with the request

                    } catch (SocketTimeoutException e) {
                        if (network_changed) {
                            // TODO: Send the new network configuration
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (Exception e) {
                System.out.println("Exception escaped");
                e.printStackTrace();
            }
            alive = false;
        }
    }

    void waitUntilAllWorkersAreReady(int requestedWorkers) {
        while (workers.size() < requestedWorkers) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    DataResponse waitForDataResponse() {
        System.out.println("Waiting for response");
        synchronized (response_lock) {
            try {
                response_lock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Received response 2");

        return response;
    }

    public static void main(String[] args) throws SocketException {
        /// This sends the address back to the host process
        System.out.println("ADDRESS::" + Address.getOwnAddress().withPort(CLIENT_PORT));

        Coordinator coord = new Coordinator();
        coord.start();
    }
}
