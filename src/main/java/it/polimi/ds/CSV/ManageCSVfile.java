package it.polimi.ds.CSV;

import com.google.protobuf.ByteString;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.OperatorName;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * The ManageCSVfile class provides methods for reading input tuples and
 * operations from a CSV file.
 */
public class ManageCSVfile {
    /**
     * Reads the number of partition required from the dataflow.
     *
     * @param csvFile the input file.
     * @return the number of partition.
     */
    public static Integer readCSVpartition(File csvFile) {
        if (csvFile.length() == 0) {
            return 0;
        }
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] records = reader.readNext();

            return Integer.parseInt(records[0].split(";")[0]);
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Reads the input tuples from CSV file.
     *
     * @param csvFile the CSV input file.
     * @return the input tuple.
     */
    public static List<Pair<Integer, Integer>> readCSVinput(File csvFile) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // reads all input tuple
            for (int i = 1; i < records.size(); i++) {
                result.add(new Pair<>(Integer.parseInt(records.get(i)[0].split(";")[0]),
                        Integer.parseInt(records.get(i)[0].split(";")[1])));
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static List<Pair<Integer, Integer>> readCSVinput(ByteString data) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        //TODO da scrivere

        return result;
    }

    /**
     * Reads the input tuples from CSV file.
     *
     * @param csvFile the CSV input file.
     * @return the input tuple.
     */
    public static List<Pair<Integer, Integer>> readCSVresult(File csvFile) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // reads all input tuple
            for (int i = 0; i < records.size(); i++) {
                result.add(new Pair<>(Integer.parseInt(records.get(i)[0].split(";")[0]),
                        Integer.parseInt(records.get(i)[0].split(";")[1])));
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Reads the input tuples from CSV file.
     *
     * @param csvFile the CSV input file.
     * @return the input tuple.
     */
    public static List<Pair<Integer, Integer>> readCSVinput(File csvFile, int start, int end) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // reads all input tuple
            for (int i = start - 1; i < end; i++) {
                result.add(new Pair<>(Integer.parseInt(records.get(i)[0].split(";")[0]),
                        Integer.parseInt(records.get(i)[0].split(";")[1])));
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Reads the input operation from CSV file.
     *
     * @param csvFile the CSV input file.
     * @return the operation to be carried out.
     */
    public static List<Triplet<OperatorName, FunctionName, Integer>> readCSVoperation(File csvFile) {
        List<Triplet<OperatorName, FunctionName, Integer>> result = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // reads all operation to perform - start from the last tuple input
            for (int i = 0; i < records.size(); i++) {
                result.add(new Triplet<>(OperatorName.getsEnumerationValue(records.get(i)[0].split(";")[0]),
                        FunctionName.getsEnumerationValue(records.get(i)[0].split(";")[1]),
                        Integer.parseInt(records.get(i)[0].split(";")[2])));
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static List<Triplet<OperatorName, FunctionName, Integer>> readCSVoperation(ByteString operation) {
        //list of operation to return
        List<Triplet<OperatorName, FunctionName, Integer>> operations = new ArrayList<>();

        //TODO da scrivere

        return operations;
    }

    /**
     * Checks if a CSV file has the correct format for input.
     * It verifies that the first row contains only one element (number of partition),
     * and all subsequent rows contain two elements (key | data).
     *
     * @param csvFile the input file that contains the data.
     * @return true if the file format is correct, false otherwise.
     */
    public static boolean checkCSVinput(File csvFile) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // the 1st row represents the input file format
            if (records.get(0)[0].split(";").length != 1) {
                return false;
            }

            // reads all data
            for (int i = 1; i < records.size(); i++) {
                if (records.get(i)[0].split(";").length != 2) {
                    return false;
                }
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Checks if a CSV file has the correct format for input.
     * It verifies that all rows contain three elements (operation | function | quantity).
     *
     * @param csvFile the input file that contains the data.
     * @return true if the file format is correct, false otherwise.
     */
    public static boolean checkCSVoperator(File csvFile) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();

            // reads all operation
            for (String[] record : records) {
                if (record[0].split(";").length != 3) {
                    return false;
                }
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Method takes a list of CSV files as input and concatenates their contents
     * into a single file.
     *
     * @param paths the list of all input data to concatenate.
     * @return a concatenate file.
     */
    public static File concatenateCSVfile(List<String> paths) {
        StringBuilder completeData = new StringBuilder();

        for (int i = 0; i < paths.size(); i++) {
            try {
                completeData.append(Files.readString(Paths.get(paths.get(i))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File outputFile = new File(paths.get(0));

        try {
            outputFile.createNewFile();
            FileWriter writer = new FileWriter(outputFile);
            writer.write(completeData.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputFile;
    }
}

/*
 * CSV INPUT file format:
 * number of partition
 * key | value
 * key | value
 * ...
 *
 * CSV OPERATION file format:
 * Operation | Function | Quantity
 */
