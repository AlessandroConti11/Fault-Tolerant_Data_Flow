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
    public static void main2(String[] args) throws UnknownHostException, IOException {
        // The path of the program to execute.
        String program;
        // The path of the data to be used in the computation.
        String dataString;
        // The data to be used in the computation
        List<Pair<Integer, Integer>> data;
        // Read the user input.
        Scanner scanner = new Scanner(System.in);
        // The counter of the number of executions.
        int counter = 0;

        if (args.length == 1) {
            program = insertProgram();
            dataString = insertData();
        } else if (args.length == 3) {
            program = args[1];
            dataString = args[2];
        } else {
            program = args[1];
            dataString = insertData();
        }

        while (true) {
            Request request = null;
            request = new RequestBuilder()
                    .setAllocations(2)
                    .setProgram(ByteString.copyFromUtf8(Files.readString(Paths.get(program))))
                    .addAllocators(Arrays.asList(args).stream()
                            .map(Address::fromStringIp)
                            .map(a -> a.getValue0().withPort(Allocator.PORT))
                            .collect(Collectors.toList()))
                    .allocate();

            data = ManageCSVfile.readCSVinput(new File(dataString));

            request.sendData(data);

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
            request.getResponses().forEach(System.out::println);

            ManageCSVfile.writeCSVresult(request.getResponses(), "compute" + counter + ".csv");
            counter++;

            request.close();

            System.out.println("Do you want insert a new program? (y/n)");
            if (scanner.nextLine().toLowerCase().equals("n")) {
                break;
            } else {
                program = insertProgram();
                dataString = insertData();
            }
        }
    }

    /// Nel caso la metto nel commit lasciami sto main che è comodo per testare la
    /// roba
    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
        Request request = new RequestBuilder()
                .setAllocations(2)
                .setProgram(ByteString.copyFromUtf8("filter;not_equal;55\n" +
                        "change_key;add;70\n" +
                        "map;add;13\n" +
                        "filter;lower_or_equal;93\n" +
                        "filter;not_equal;11\n" +
                        "map;mult;77\n" +
                        "filter;not_equal;19\n" +
                        "change_key;add;75\n" +
                        "reduce;add;40\n" +
                        ""))
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
