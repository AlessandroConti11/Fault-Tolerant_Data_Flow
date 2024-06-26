package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.NodeManagerInfo;

public class Coordinator {

    public static int CLIENT_PORT = 5000;
    public static int WORKER_PORT = 5001;

    private ConcurrentMap<Long, WorkerManagerHandler> workers = new ConcurrentHashMap<>();

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

    Thread clientListener = new Thread(() -> {
        try {
            ServerSocket clientListener = new ServerSocket(CLIENT_PORT);
            Node conn = new Node(clientListener.accept());
            var allocReq = conn.receive(AllocationRequest.class);
            List<Address> hosts = allocReq.getAllocatorsList().stream().map(a -> new Address(a))
                    .collect(Collectors.toList());

            // TODO: Maybe see how to split the work between the hosts
            Node h = new Node(hosts.get(0));
            h.send(AllocateNodeManagerRequest.newBuilder()
                    .setNodeManagerInfo(NodeManagerInfo.newBuilder()
                            .setAddress(Address.getOwnAddress().withPort(WORKER_PORT).toProto())
                            .setNumContainers(allocReq.getNumberOfTasks()).build())
                    .build());

            var resp = h.receive(AllocateNodeManagerResponse.class);

            conn.send(AllocationResponse.newBuilder().build());

            while (true) {
                try {
                    var req = conn.receive(DataRequest.class);
                    // TODO: Do something with the request
                    conn.send(DataResponse.newBuilder().build());
                } catch (Exception e) {
                    System.out.println("Client disconnected");
                    break;
                }
            }

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    Thread workerListener = new Thread(() -> {
        ExecutorService executors = Executors.newCachedThreadPool();

        try {
            ServerSocket workerListener = new ServerSocket(WORKER_PORT);
            while (true) {
                long id = 0;
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
        private Node conn;
        public static final int CHECKPOINT_TIMEOUT = 1000;
        private volatile boolean networkChanged = false;
        private volatile boolean alive = true;

        public WorkerManagerHandler(Node conn) {
            this.conn = conn;
        }

        public boolean checkAlive() {
            return alive;
        }

        public void notifyNetworkChange() {
            networkChanged = true;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        var req = conn.receive(CheckpointRequest.class, CHECKPOINT_TIMEOUT);
                        // TODO: Do something with the request
                    } catch (SocketTimeoutException e) {
                        if (networkChanged) {
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

    public static void main(String[] args) throws SocketException {
        /// This sends the address back to the host process
        System.out.println("ADDRESS::" + Address.getOwnAddress().withPort(CLIENT_PORT));

        Coordinator coord = new Coordinator();
        coord.start();
    }
}
