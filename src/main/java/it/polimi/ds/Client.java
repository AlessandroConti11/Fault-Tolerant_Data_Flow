package it.polimi.ds;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import it.polimi.ds.proto.AllocateNodeManagerRequest;

public class Client {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Node n = new Node(new Socket("localhost", Host.PORT));

        n.send(AllocateNodeManagerRequest.newBuilder().setCount(1).build());

        System.out.println("Connected to server");
    }
}
