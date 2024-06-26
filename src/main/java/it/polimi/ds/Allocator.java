package it.polimi.ds;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.ServerSocket;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.NodeManagerInfo;

public class Allocator {

    public static final int PORT = 9090;
    public static int procCounter = 0;

    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(PORT);
        ProcessBuilder process_builder = new ProcessBuilder("mvn")
                .redirectErrorStream(true);

        System.out.println("Server is running on " + Address.getOwnAddress().toString());

        while (true) {
            Node conn = new Node(listener.accept());
            procCounter++;
            int procId = procCounter;

            new Thread(() -> {
                try {
                    var req = conn.receive(AllocateNodeManagerRequest.class);
                    if (req.hasCoordinator() && req.getCoordinator() == true) {
                        System.out.println("Coordinator");

                        Process proc = process_builder
                                .command("java", "-jar", "target/coordinator.jar")
                                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                                .start();

                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(proc.getInputStream()));

                        String line = reader.readLine();
                        Address coord_addr = Address.fromString(line.split("::")[1]).getValue0();

                        conn.send(AllocateNodeManagerResponse.newBuilder()
                                .setAddress(coord_addr.toProto())
                                .build());

                        spoofOutput(proc, "[COORDINATOR(" + procId + ")] ");
                    } else if (req.hasNodeManagerInfo()) {
                        NodeManagerInfo info = req.getNodeManagerInfo();
                        for (int i = 0; i < info.getNumContainers(); i++) {
                            Process proc = process_builder
                                    .command("java", "-jar", "target/workers.jar",
                                            new Address(info.getAddress()).toString())
                                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                                    .start();

                            spoofOutput(proc, "[WORKER(" + procId + ")] ");
                        }

                        conn.send(AllocateNodeManagerResponse.newBuilder().build());
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }

    }

    static void spoofOutput(Process p, String prefix) {
        new Thread(() -> {
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(p.getInputStream()));

                String line = reader.readLine();
                while (line != null) {
                    System.out.println(prefix + line);
                    line = reader.readLine();
                }

            } catch (IOException e) {
                System.out.println(prefix + "Error reading output, closing pipe");
            }
        }).start();
    }
}
