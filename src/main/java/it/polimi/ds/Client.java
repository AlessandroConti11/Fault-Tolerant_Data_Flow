package it.polimi.ds;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.util.stream.Collectors;

import org.javatuples.Pair;

import com.google.protobuf.ByteString;

import it.polimi.ds.RequestBuilder.Request;
import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataResponse;

public class Client {
    /**
     * Reads the program file.
     *
     * @return the content of program file.
     */
    public static String insertProgram() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Insert the path of the program file: ");
        return scanner.nextLine();
    }

    /**
     * Reads the data file.
     *
     * @return the content of data file.
     */
    public static String insertData() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Insert the path of the data file: ");
        return scanner.nextLine();
    }

    /**
     * Checks if the result of the computation is correct.
     *
     * @param computed   the result computed.
     * @param realResult the real result
     * @return the string explaining whether the result is correct.
     */
    public static String checkResult(Vector<DataResponse> computed, File realResult) {
        // The result.
        List<Pair<Integer, Integer>> real = ManageCSVfile.readCSVresult(realResult);
        // The data response to check.
        DataResponse dataResponse;

        for (Pair<Integer, Integer> objects : real) {
            dataResponse = DataResponse.newBuilder()
                    .addData(Data.newBuilder()
                            .setKey(objects.getValue0())
                            .setValue(objects.getValue1())
                            .build())
                    .build();

            if (!computed.contains(dataResponse)) {
                return "The result is wrong";
            }
        }

        return "The result is correct";
    }

    /**
     * Client main.
     *
     * @param args args[0] = server_address; args[1] = program_path; args[2] =
     *             data_path
     */
    public static void main(String[] args) throws UnknownHostException, IOException {
        // The path of the program to execute.
        String program = null;
        // The path of the data to be used in the computation.
        String dataString = null;
        // The data to be used in the computation
        List<Pair<Integer, Integer>> data;
        // Read the user input.
        Scanner scanner = new Scanner(System.in);
        // The counter of the number of executions.
        int counter = 0;
        int i = 0;
        List<Address> addresses = new ArrayList();
        List<String> data_strings = new ArrayList();

        try {
            for (var a : args) {
                addresses.add(Address.fromStringIp(a).getValue0().withPort(Allocator.PORT));
                i += 1;
            }
        } catch (Exception e) {
            if (i < args.length) {
                program = args[i];
                i += 1;
                for (; i < args.length; i++) {
                    data_strings.add(args[i]);
                }

                if (data_strings.size() == 0) {
                    dataString = insertData();
                } else {
                    dataString = data_strings.removeFirst();
                }
            } else {
                program = insertProgram();
                dataString = insertData();
            }
        }

        int allocations = -1;
        while (allocations < 0) {
            System.out.println("How many allocators do you want? Positive number please :^)");
            allocations = Integer.parseInt(scanner.nextLine());
        }

        if (program == null) {
            System.out.println("Please provide a program");
            System.exit(1);
        }

        if (dataString == null) {
            System.out.println("Please provide a data file");
            System.exit(1);
        }

        while (true) {
            Request request = null;
            request = new RequestBuilder()
                    .setAllocations(allocations)
                    .setProgram(ByteString.copyFromUtf8(Files.readString(Paths.get(program))))
                    .addAllocators(addresses)
                    .allocate();

            data = ManageCSVfile.readCSVinput(new File(dataString));

            request.sendData(data);

            for (var d : data_strings) {
                request.sendData(ManageCSVfile.readCSVinput(new File(d)));
            }
            data_strings.clear();

            while (true) {
                System.out.println("Do you want to insert other data to compute? (y/n)");
                if (scanner.nextLine().toLowerCase().equals("n")) {
                    break;
                } else {
                    dataString = insertData();
                    request.sendData(ManageCSVfile.readCSVinput(new File(dataString)));
                }
            }

            System.out.println("\n\nThe result:");
            var resp = request.getResponses();
            for (var r : resp) {
                System.out.println("Got result for " + r.getComputationId());
            }

            ManageCSVfile.writeCSVresult(request.getResponses(), "compute" + counter + ".csv");
            counter++;

            request.close();

            System.out.println("Do you want insert a new program? (y/n)");
            if (scanner.nextLine().toLowerCase().equals("n")) {
                System.exit(0);
            } else {
                program = insertProgram();
                dataString = insertData();
            }
        }
    }

    /// Nel caso la metto nel commit lasciami sto main che è comodo per testare la
    /// roba
    public static void main2(String[] args) throws UnknownHostException, IOException, InterruptedException {
        Request request = new RequestBuilder()
                .setAllocations(2)
                .setProgram(ByteString.copyFromUtf8(
                        "change_key;mult;67\n"
                                + "map;mult;12\n"
                                + "change_key;sub;14\n"
                                + "map;sub;17\n"
                                + "filter;not_equal;47\n"
                                + "filter;not_equal;11\n"
                                + "change_key;add;62\n"
                                + "map;add;46\n"
                                + "filter;not_equal;87\n"
                                + "filter;not_equal;83\n"
                                + "filter;not_equal;98\n"
                                + "filter;not_equal;13\n"
                                + "change_key;mult;76\n"
                                + ""))
                .addAllocators(Arrays.asList(args).stream()
                        .map(Address::fromStringIp)
                        .map(a -> a.getValue0().withPort(Allocator.PORT))
                        .collect(Collectors.toList()))
                .allocate();

        List<Pair<Integer, Integer>> data = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            data.add(new Pair<>(i, i));
        }
        // data.add(new Pair<>(1, 1));
        // data.add(new Pair<>(2, 2));

        // request.sendData(data);
        // data.add(new Pair<>(3, 3));
        request.sendData(data);
        // data.add(new Pair<>(4, 2));
        // request.sendData(data);
        // data.add(new Pair<>(5, 5));
        // request.sendData(data);

        request.getResponses().forEach(System.out::println);
        request.close();
    }
}
