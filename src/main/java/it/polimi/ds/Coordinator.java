package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.Directed_Acyclic_Graph.ManageDAG;
import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.ClientRequest;
import it.polimi.ds.proto.CloseResponse;
import it.polimi.ds.proto.Computation;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.NodeManagerInfo;
import it.polimi.ds.proto.Operation;
import it.polimi.ds.proto.ProtoTask;
import it.polimi.ds.proto.RegisterNodeManagerRequest;
import it.polimi.ds.proto.RegisterNodeManagerResponse;
import it.polimi.ds.proto.ReturnCode;
import it.polimi.ds.proto.SynchRequest;
import it.polimi.ds.proto.SynchResponse;
import it.polimi.ds.proto.UpdateNetworkRequest;
import it.polimi.ds.proto.UpdateNetworkResponse;

public class Coordinator {

    public static int CLIENT_PORT = 5000;
    public static int WORKER_PORT = 5001;

    private ConcurrentMap<Long, WorkerManagerHandler> workers = new ConcurrentHashMap<>();
    private ByteString program; // TODO: Change this into the actual type after parsing step
    private List<Address> allocators;

    private Object response_lock = new Object();
    private DataResponse response = null;

    private ManageDAG dag = null;

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
            waitUntilAllWorkersAreReady(dag.getNumberOfTaskManager());

            /// Send back the OK to the client, this will signal that the network is ready
            client.send(AllocationResponse.newBuilder()
                    .build());

            while (true) {
                try {
                    var req = client.receive(ClientRequest.class);
                    if (req.hasDataRequest()) {
                        System.out.println("Received data request " + req.getDataRequest().getDataList());

                        dag.setData(ManageCSVfile.readCSVinput(req.getDataRequest().getDataList()));

                        dag.getTasksOfGroup(0).parallelStream().forEach(t -> {
                            try {
                                workers.get((long) t).send(req.getDataRequest());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });

                        client.send(waitForDataResponse());
                    } else if (req.hasCloseRequest()) {
                        System.out.println("Closing connection");
                        client.send(CloseResponse.newBuilder().build());
                        System.exit(0);
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
                Node node = new Node(workerListener.accept());
                long id = dag.getNextFreeTaskManager().orElseThrow(); // TODO: Handle this case, in theory it should
                                                                      // never happen, but you never know. This happens
                                                                      // when we initialize to many workerManagers
                                                                      // somehow
                workers.put(id, new WorkerManagerHandler(node, id));
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
                int crashed = 0;
                for (var worker : workers.entrySet()) {
                    synchronized (lock) {
                        boolean alive = worker.getValue().checkAlive();
                        if (!alive) {
                            dag.addFreeTaskManager((int) (long) worker.getKey());
                            workers.remove(worker.getKey());
                            crashed++;
                        }
                    }

                }

                if (crashed > 0) {
                    allocateResources(crashed);
                    waitUntilAllWorkersAreReady(dag.getNumberOfTaskManager());

                    // TODO: Figure out how to send the checkpoint data
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
        private volatile boolean alive = false;

        private final boolean is_last = true;

        private long id;
        private Address address;

        public WorkerManagerHandler(Node conn, long id) throws IOException {
            this.control_connection = conn;
            this.id = id;

            var registration = conn.receive(RegisterNodeManagerRequest.class);
            this.address = new Address(registration.getAddress());

            List<Long> tasks = dag.getTasksOfTaskManager((int) id);
            System.out.println("id: " + id);
            System.out.println("tasks: " + dag.getTasksInGroup());
            System.out.println("tasks: " + tasks);
            System.out.println("operations: " + dag.getOperationsForTaskManager(id));
            var operations = dag.getOperationsForTaskManager(id);

            /// WARNING: I don't want to touch this thing, I'm scared of it
            conn.send(RegisterNodeManagerResponse.newBuilder()
                    .setId(id)
                    .addAllTasks(tasks.stream()
                            .map(t -> ProtoTask.newBuilder()
                                    .setId(t)
                                    .setGroupId(dag.groupFromTask((long) t).get())
                                    .setIsCheckpoint(0) // TODO: Fix this
                                    .build())
                            .collect(Collectors.toList()))
                    .addAllComputations(operations.stream()
                            .map(op -> Computation.newBuilder()
                                    .setGroupId(op.getValue1())
                                    .addAllManagerSuccessorIds(
                                            dag.getManagersOfNextGroup((long) op.getValue1()).stream()
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
                    + address.withPort(WorkerManager.DATA_PORT + (int) id));
            data_connection = new Node(address.withPort(WorkerManager.DATA_PORT + (int) id));

            alive = true;
        }

        public boolean checkAlive() {
            return alive;
        }

        public boolean isReady() {
            return alive && network_changed;
        }

        public void notifyNetworkChange() {
            network_changed = true;
        }

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
                        // TODO: dag.putChecckpointData();
                        // Also, we need dag.getCheckpointData();

                    } catch (SocketTimeoutException e) {
                        if (network_changed) {
                            control_connection.send(UpdateNetworkRequest.newBuilder()
                                    .addAllTaskManagerIds(workers.keySet())
                                    .addAllAddresses(workers.values().stream()
                                            .map(w -> w.address.toProto())
                                            .collect(Collectors.toList()))
                                    .build());

                            var ok = control_connection.receive(UpdateNetworkResponse.class);
                            network_changed = false;
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

    void allocateResources(int requestedWorkers) throws IOException {
        Node h = new Node(allocators.get(0));
        h.send(AllocateNodeManagerRequest.newBuilder()
                .setNodeManagerInfo(NodeManagerInfo.newBuilder()
                        .setAddress(Address.getOwnAddress().withPort(WORKER_PORT).toProto())
                        .setNumContainers(requestedWorkers).build())
                .build());

        var resp = h.receive(AllocateNodeManagerResponse.class);

        /// Wait until all the workers are ready, then, once we have collected all the
        /// network information, notify the WorkerManagers of all the others' addresses
        waitUntilAllWorkersAreReady(dag.getNumberOfTaskManager());
        workers.forEach((k, v) -> {
            v.notifyNetworkChange();
        });
    }

    void waitUntilAllWorkersAreReady(int requestedWorkers) {
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
