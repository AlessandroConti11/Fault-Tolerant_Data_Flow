package it.polimi.ds;

import java.io.IOException;
import java.net.ServerSocket;

import it.polimi.ds.proto.AllocateNodeManagerRequest;

public class Host {

    public static final int PORT = 9090;

    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(PORT);
        ProcessBuilder process_builder = new ProcessBuilder("echo", "TODO: Start NodeManagers!")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT);

        System.out.println("Server is running on port " + PORT);

        while (true) {
            Node conn = new Node(listener.accept());

            new Thread(() -> {
                try {
                    var req = conn.receive(AllocateNodeManagerRequest.class);
                    for (int i = 0; i < req.getCount(); i++) {
                        process_builder.start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
