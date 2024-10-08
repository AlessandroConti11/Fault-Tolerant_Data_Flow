package it.polimi.ds;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.NodeManagerInfo;

public class Allocator {

    public static final int PORT = 9090;
    public static AtomicInteger procCounter = new AtomicInteger(0);

    private static List<Process> procs = new Vector<>();

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        final long ID = SecureRandom.getInstanceStrong().nextLong();

        try (ServerSocket listener = new ServerSocket(PORT)) {

            System.out.println("Server is running on " + Address.getOwnAddress().withPort(PORT).toString());

            while (true) {
                Node conn = new Node(listener.accept());

                // allocate Coordinator / Workers
                new Thread(() -> {
                    ProcessBuilder process_builder = new ProcessBuilder("mvn")
                            .redirectErrorStream(true);

                    try {
                        var req = conn.receive(AllocateNodeManagerRequest.class);
                        if (req.hasCoordinator() && req.getCoordinator() == true) {
                            int procId = procCounter.addAndGet(2);
                            Process proc = process_builder
                                    .command("java", "-ea", "-jar", "target/coordinator.jar", Integer.toString(procId))
                                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                                    .start();

                            procs.add(proc);

                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(proc.getInputStream()));

                            String line = reader.readLine();
                            Address coord_addr = Address.fromString(line.split("::")[1]).getValue0();

                            conn.send(AllocateNodeManagerResponse.newBuilder()
                                    .setAddress(coord_addr.toProto())
                                    .build());

                            spoofOutput(proc,
                                    "[" + colors[procId % colors.length] + "COORDINATOR(" + procId + ")" + RESET
                                            + "] ");
                        } else if (req.hasNodeManagerInfo()) {
                            NodeManagerInfo info = req.getNodeManagerInfo();
                            for (int i = 0; i < info.getNumContainers(); i++) {
                                int procId = procCounter.addAndGet(1);

                                Process proc = process_builder
                                        .command("java", "-ea", "-jar", "target/workers.jar",
                                                new Address(info.getAddress()).toString(), Integer.toString(procId),
                                                Long.toString(ID))
                                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                                        .start();

                                procs.add(proc);

                                spoofOutput(proc,
                                        "[" + colors[(procId + i) % colors.length] + "WORKER(" + (procId + i) + ")"
                                                + RESET
                                                + "] ");
                            }

                            conn.send(AllocateNodeManagerResponse.newBuilder().build());
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final String WM_MESSAGE_PREFIX = "WMID";

    static void spoofOutput(Process p, String prefix) {
        new Thread(() -> {
            String pr = prefix;
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(p.getInputStream()));

                String line = reader.readLine();
                while (line != null) {
                    if (line.startsWith(WM_MESSAGE_PREFIX)) {
                        String id = line.substring(WM_MESSAGE_PREFIX.length());
                        pr = pr.replace(")", ", " + id + ")");
                        line = reader.readLine();
                        continue;
                    }
                    // [N/M][PROCESS]WHAT IS DOING
                    System.out.println(getAlivePrefix() + pr + line);
                    line = reader.readLine();
                }

            } catch (IOException e) {
                System.out.println(getAlivePrefix() + prefix + "Error reading output, closing pipe");
            }
            procs.remove(p);
            System.out.println(getAlivePrefix() + RED + "****EXIT**** " + RESET + prefix);
        }).start();
    }

    private static String getAlivePrefix() {
        int count = 0;
        for (int i = 0; i < procs.size(); i++) {
            var p = procs.get(i);
            count += p.isAlive() ? 1 : 0;
        }

        return "[" + count + "/" + procs.size() + "]";
    }

    public static final String RESET = "\033[0m"; // Text Reset
    public static final String BLACK = "\033[0;30m"; // BLACK
    public static final String RED = "\033[0;31m"; // RED
    public static final String GREEN = "\033[0;32m"; // GREEN
    public static final String YELLOW = "\033[0;33m"; // YELLOW
    public static final String BLUE = "\033[0;34m"; // BLUE
    public static final String PURPLE = "\033[0;35m"; // PURPLE
    public static final String CYAN = "\033[0;36m"; // CYAN
    public static final String WHITE = "\033[0;37m"; // WHITE

    private static final String[] colors = {
            BLACK, RED, GREEN, YELLOW, BLUE, PURPLE, CYAN, WHITE
    };
}
