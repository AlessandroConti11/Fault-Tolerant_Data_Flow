package it.polimi.ds;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.RequestBuilder.Request;

import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataResponse;
import org.javatuples.Pair;

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
     * @param computed the result computed.
     * @param realResult the real result
     * @return the string explaining whether the result is correct.
     */
    public static String checkResult(Vector<DataResponse> computed, File realResult) {
        //The result.
        List<Pair<Integer, Integer>> real = ManageCSVfile.readCSVresult(realResult);
        //The data response to check.
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
     * @param args args[0] = server_address; args[1] = program_path; args[2] = data_path
     */
    public static void main(String[] args) {
        //The path of the program to execute.
        String program;
        //The path of the data to be used in the computation.
        String dataString;
        //The data to be used in the computation
        List<Pair<Integer, Integer>> data;
        //Read the user input.
        Scanner scanner = new Scanner(System.in);
        //The counter of the number of executions.
        int counter = 0;

        if (args.length == 1) {
            program = insertProgram();
            dataString = insertData();
        }
        else if (args.length == 3) {
            program = args[0];
            dataString = args[1];
        }
        else {
            program = args[1];
            dataString = insertData();
        }

        while (true) {
            Request request = null;
            try {
                request = new RequestBuilder()
                        .setAllocations(2)
                        .setProgram(ByteString.copyFromUtf8(Files.readString(Paths.get(program))))
                        .addAllocators(Arrays.asList(args).stream()
                                .map(Address::fromStringIp)
                                .map(a -> a.getValue0().withPort(Allocator.PORT))
                                .collect(Collectors.toList()))
                        .allocate();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            data = ManageCSVfile.readCSVinput(new File(dataString));

            try {
                request.sendData(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("\n\nThe result:");
            request.getResponses().forEach(System.out::println);

            ManageCSVfile.writeCSVresult(request.getResponses(), "compute" + counter + ".csv");
            counter++;

            try {
                request.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Do you want to continue? (y/n)");
            if (scanner.nextLine().toLowerCase().equals("n")) {
                break;
            }
            else {
                program = insertProgram();
                dataString = insertData();
            }
        }
    }
}
