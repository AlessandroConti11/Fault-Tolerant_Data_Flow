package it.polimi.ds.CSV;

import it.polimi.ds.function.Function;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


public class CreateCSV {
    /**
     * Max number of partition to create.
     */
    private static int partitionNumber = 10;

    /**
     * Max number of data to create.
     */
    private static int maxNumberOfData = 10000;

    /**
     * Max value that key and value can assume in the data file.
     */
    private static int maxNumberDataValue = 100;

    /**
     * Number of possible operation.
     * Map,
     * Filter,
     * Change key,
     * Reduce.
     */
    private static int numberOfOperation = 4;

    /**
     * Directory where to save the file.
     */
    private static String testCaseDirName = "testCase/";


    /**
     * Getter --> gets the directory where to save the files.
     *
     * @return the directory where to save the files.
     */
    public static String getTestCaseDirName() {
        return testCaseDirName;
    }

    /**
     * Generates a string of random data.
     * The first row is the number of partition, the other rows are the pair key, value of single data.
     *
     * @return the string of random data.
     */
    public static String generateData() {
        String data = "";

        Random rand = new Random();

        for (int i = 0; i < rand.nextInt(maxNumberOfData)-1; i++) {
            data += (rand.nextInt(maxNumberDataValue)+1) + ";" + (rand.nextInt(maxNumberDataValue)+1) + "\n"; // key;value
        }
        data += (rand.nextInt(maxNumberDataValue)+1) + ";" + (rand.nextInt(maxNumberDataValue)+1); // key;value
        return data + "\n";
    }

    /**
     * Generates a string of random operation.
     *
     * @return the string of random operation.
     */
    public static String generateOperation() {
        String operation = "";
        int operationNumber;

        Random rand = new Random();

        for (int i = 0; i < rand.nextInt(maxNumberOfData)-1; i++) {
            operationNumber = rand.nextInt(numberOfOperation-1);

            switch (operationNumber) {
                case 0: // map;OPERATION;QUANTITY
                    operation += "map;";
                    switch (rand.nextInt(3)) {
                        case 0:
                            operation += "add;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 1:
                            operation += "sub;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 2:
                            operation += "mult;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                    }
                    break;
                case 1: // filter;OPERATION;QUANTITY
                    operation += "filter;";
                    switch (rand.nextInt(500)) {
                        case 0:
                            operation += "equal;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 2, 7, 12:
                            operation += "greater_than;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 3, 8, 13, 17:
                            operation += "greater_of_equal;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 4, 9, 14:
                            operation += "lower_than;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 5, 10, 15, 18:
                            operation += "lower_or_equal;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        default:
                            operation += "not_equal;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                    }
                    break;
                case 2: // change_key;OPERATION;QUANTITY
                    operation += "change_key;";
                    switch (rand.nextInt(3)) {
                        case 0:
                            operation += "add;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 1:
                            operation += "sub;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                        case 2:
                            operation += "mult;" + rand.nextInt(maxNumberDataValue) + "\n";
                            break;
                    }
                    break;
            }
        }
        operationNumber = rand.nextInt(numberOfOperation);
        switch (operationNumber) {
            case 0: // map;OPERATION;QUANTITY
                operation += "map;";
                switch (rand.nextInt(3)) {
                    case 0:
                        operation += "add;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 1:
                        operation += "sub;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 2:
                        operation += "mult;" + rand.nextInt(maxNumberDataValue);
                        break;
                }
                break;
            case 1: // filter;OPERATION;QUANTITY
                operation += "filter;";
                switch (rand.nextInt(6)) {
                    case 0:
                        operation += "equal;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 1:
                        operation += "not_equal;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 2:
                        operation += "greater_than;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 3:
                        operation += "greater_of_equal;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 4:
                        operation += "lower_than;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 5:
                        operation += "lower_or_equal;" + rand.nextInt(maxNumberDataValue);
                        break;
                }
                break;
            case 2: // change_key;OPERATION;QUANTITY
                operation += "change_key;";
                switch (rand.nextInt(3)) {
                    case 0:
                        operation += "add;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 1:
                        operation += "sub;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 2:
                        operation += "mult;" + rand.nextInt(maxNumberDataValue);
                        break;
                }
                break;
            case 3: // reduce;OPERATION;QUANTITY
                operation += "reduce;";
                switch (rand.nextInt(3)) {
                    case 0:
                        operation += "add;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 1:
                        operation += "sub;" + rand.nextInt(maxNumberDataValue);
                        break;
                    case 2:
                        operation += "mult;" + rand.nextInt(maxNumberDataValue);
                        break;
                }
                break;
        }

        return operation + "\n";
    }


    /**
     * Generate a string of results pair.
     *
     * @param data the input data.
     * @param operation the operation to compute.
     * @return the string of results.
     */
    public static String generateResult(File data, File operation) {
        String result = "";

        //Save the tuple needed in the computation
        List<Pair<Integer, Integer>> tuples = ManageCSVfile.readCSVinput(data);
        //Save the operation
        List<Triplet<OperatorName, FunctionName, Integer>> operations = ManageCSVfile.readCSVoperation(operation);

        tuples = new Operator().operations(operations, tuples);

        for (int i = 0; i < tuples.size(); i++) {
            result += tuples.get(i).getValue0() + ";" + tuples.get(i).getValue1() + "\n";
        }

        return result;
    }

    /**
     * Generates a file that has fileName as Name and fileData as Data.
     *
     * @param fileData the data to write.
     * @param fileName the file name.
     */
    public static File generateFile(String fileData, String fileName) {
        File directory = new File(testCaseDirName);
        if (!directory.exists()) {
            directory.mkdirs(); // This will create the directory if it does not exist
        }

        File file = new File(directory, fileName);

        try {
            file.createNewFile();
            FileWriter fw = new FileWriter(file);
            fw.write(fileData);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return file;
    }


    /**
     * CreateCSV main.
     *
     * @param args args[0] = start; args[1] = end
     */
    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            // create data file
            String data = generateData();
            File dataFile = generateFile(data, ("data" + i + ".csv"));

            //create operation file
            String operation = generateOperation();
            File operationFile = generateFile(operation, ("operation" + i + ".csv"));

            //create result file
            String result = generateResult(dataFile, operationFile);
            File resultFile = generateFile(result, ("result" + i + ".csv"));
        }
    }
}
