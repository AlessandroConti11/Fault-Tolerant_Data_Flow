package it.polimi.ds;

import java.net.ServerSocket;
import java.net.SocketException;
import java.util.List;
import java.util.stream.Collectors;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.NodeManagerInfo;

public class Coordinator {

    public static int CLIENT_PORT = 5000;
    public static int WORKER_PORT = 5001;

    public Coordinator() {
    }

    Thread clientListener = new Thread(() -> {
        try {
            ServerSocket clientListener = new ServerSocket(CLIENT_PORT);
            Node conn = new Node(clientListener.accept());
            var allocReq = conn.receive(AllocationRequest.class);
            List<Address> hosts = allocReq.getAllocatorsList()
                    .stream().map(a -> new Address(a))
                    .collect(Collectors.toList());

            // TODO: Maybe see how to split the work between the hosts
            Node h = new Node(hosts.get(0));
            h.send(AllocateNodeManagerRequest.newBuilder()
                    .setNodeManagerInfo(
                            NodeManagerInfo.newBuilder()
                                    .setAddress(Address.getOwnAddress().withPort(WORKER_PORT).toProto())
                                    .setNumContainers(allocReq.getNumberOfTasks())
                                    .build())
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
        try {
            ServerSocket workerListener = new ServerSocket(WORKER_PORT);
            // TODO: Make threadpool to accept the new workers, and put them in wait for the
            // tasks. Then compute the DAG and begin execution
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    public static void main(String[] args) throws SocketException {
        /// This sends the address back to the host process
        System.out.println("ADDRESS::" + Address.getOwnAddress().withPort(CLIENT_PORT));

        Coordinator coord = new Coordinator();
    }
}
