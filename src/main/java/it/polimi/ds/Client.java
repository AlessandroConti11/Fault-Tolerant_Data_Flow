package it.polimi.ds;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;

public class Client {
    public Node n;

    public Client() throws UnknownHostException, IOException {
        n = new Node(new Socket("localhost", Host.PORT));
    }

    public void createCoordinator() throws IOException {
        n.send(AllocateNodeManagerRequest.newBuilder().setCoordinator(true).build());
    }

    public static void main(String[] args) throws UnknownHostException, IOException {
        Client c = new Client();

        c.createCoordinator();
        // c.n.send(AllocateNodeManagerRequest.newBuilder()
        // .setNodeManagerInfo(NodeManagerInfo.newBuilder()
        // .setAddress(ProtoAddress.newBuilder()
        // .setIp("192.168.1.1")
        // .setPort(Host.PORT)
        // .setMask(24)
        // .build())
        // .setNumContainers(1)
        // .build())
        // .build());

        var resp = c.n.receive(AllocateNodeManagerResponse.class);

        System.out.println("Connected to server");
    }
}
