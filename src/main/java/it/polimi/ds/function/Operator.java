package it.polimi.ds.function;


import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.*;


/**
 * The Operator class provides function that processing large amount big-data.
 */
public class Operator {
    /**
     * Maps a function to each value in a list of pairs of integers and returns a new list of pairs with the same keys and the mapped values.
     *
     * @param input the input list.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the mapped list.
     */
    private List<Pair<Integer, Integer>> mapF(List<Pair<Integer, Integer>> input, FunctionName type, int quantity) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        for (Pair<Integer, Integer> objects : input) {
            //<k, v> --> <k, f(v)>
            result.add(new Pair<>(objects.getValue0(), Function.f(objects.getValue1(), quantity, type)));
        }

        return result;
    }

    /**
     * Maps a pair to a new pair with the same key but different value.
     *
     * @param input the input pain.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the mapped pair.
     */
    private Pair<Integer, Integer> mapF(Pair<Integer, Integer> input, FunctionName type, int quantity) {
        return new Pair<>(input.getValue0(), Function.f(input.getValue1(), quantity, type));
    }

    /**
     * Filters a list of pairs of integers based on a function and a quantity. Returns a new list of pairs with the same keys and only the pairs that satisfy the function condition.
     *
     * @param input the input list.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the filtered list.
     */
    private List<Pair<Integer, Integer>> filterF(List<Pair<Integer, Integer>> input, FunctionName type, int quantity) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        for (Pair<Integer, Integer> objects : input) {
            //<k, v> --> <k, v> iff f(v) is true
            if (Function.f(objects.getValue1(), quantity, type) == 1) {
                result.add(objects);
            }
        }

        return result;
    }

    /**
     * Filters a pair.
     *
     * @param input the input pair.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the input pair if satisfies the condition inserted.
     */
    private Pair<Integer, Integer> filterF(Pair<Integer, Integer> input, FunctionName type, int quantity) {
        return Function.f(input.getValue1(), quantity, type) == 1 ? input : null;
    }

    /**
     * Changes the key of each pair in a list of pairs of integers based on a function and a quantity. Returns a new list of pairs with the new keys and the same values.
     *
     * @param input the input list.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the list with key changed.
     */
    private List<Pair<Integer, Integer>> changeKeyF(List<Pair<Integer, Integer>> input, FunctionName type, int quantity) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();

        for (Pair<Integer, Integer> objects : input) {
            //<k, v> --> <f(v), v>
            result.add(new Pair<>(Function.f(objects.getValue1(), quantity, type), objects.getValue1()));
        }

        return result;
    }

    /**
     * Changes the key of the pair.
     *
     * @param input the input pair.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the changed key pair.
     */
    private Pair<Integer, Integer> changeKeyF(Pair<Integer, Integer> input, FunctionName type, int quantity) {
        return new Pair<>(Function.f(input.getValue1(), quantity, type), input.getValue1());
    }

    /**
     * Reduces a list of pairs of integers by applying a function to pairs with the same key. Returns a new list of pairs with the reduced values.
     *
     * @param input the input list.
     * @param type the function type.
     * @param quantity the quantity for the function.
     * @return the reduced list.
     */
    private List<Pair<Integer, Integer>> reduceF(List<Pair<Integer, Integer>> input, FunctionName type, int quantity) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        int actualKey = 0;

        //sorted input
        input.sort(new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> objects, Pair<Integer, Integer> t1) {
                return objects.getValue0().compareTo(t1.getValue0());
            }
        });

        if (!input.isEmpty()) {
            result.add(new Pair<>(input.get(0).getValue0(), input.get(0).getValue1()));
            for (int i = 1; i < input.size(); i++) {
                if (Objects.equals(input.get(i).getValue0(), input.get(i - 1).getValue0())) {
                    result.set(actualKey, new Pair<>(input.get(i).getValue0(), Function.f(result.get(actualKey).getValue1(), input.get(i).getValue1(), type)));
                }
                else {
                    actualKey++;
                    result.add(new Pair<>(input.get(i).getValue0(), input.get(i).getValue1()));
                }
            }
        }

        return result;
    }

    /**
     * Selects and executes one of the operation.
     *
     * @param operatorName the operation name.
     * @param functionName the function name.
     * @param quantity the quantity used in the function.
     * @param input the tuple list.
     * @return the list obtained after the computation.
     */
    private List<Pair<Integer, Integer>> op(OperatorName operatorName, FunctionName functionName, Integer quantity, List<Pair<Integer, Integer>> input) {
        switch (operatorName) {
            case MAP -> {
                return mapF(input, functionName, quantity);
            }
            case FILTER -> {
                return filterF(input, functionName, quantity);
            }
            case CHANGE_KEY -> {
                return changeKeyF(input, functionName, quantity);
            }
            case REDUCE -> {
                return reduceF(input, functionName, quantity);
            }
            default -> {
                return null;
            }
        }
    }


    /**
     * Performs the operation.
     *
     * @param operation the list of the operation to compute.
     * @param input the list of the tuple.
     * @return the list of the tuple after the computation.
     */
    public List<Pair<Integer, Integer>> operations(List<Triplet<OperatorName, FunctionName, Integer>> operation, List<Pair<Integer, Integer>> input) {
        List<Pair<Integer, Integer>> result = input;

        for (Triplet<OperatorName, FunctionName, Integer> objects : operation) {
            result = op(objects.getValue0(), objects.getValue1(), objects.getValue2(), result);
        }

        return result;
    }

    /**
     * Checks if the last instruction is a Reduce or not.
     *
     * @param operation the operation list.
     * @return the last operation if is a Reduce.
     */
    public static Optional<Triplet<OperatorName, FunctionName, Integer>> lastInstructionIsReduced(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        if (operation.isEmpty()){
            return Optional.empty();
        }
        else if (operation.get(operation.size() - 1).getValue0().equals(OperatorName.REDUCE)) {
            return Optional.of(operation.get(operation.size()-1));
        }
        return Optional.empty();
    }

    /**
     * Returns the number of Change Key (plus the Reduce operation if present) operation in the list of operation.
     *
     * @param operation the operation list.
     * @return the number of Change Key operations.
     */
    public static Integer numberOfChangeKeys(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        int result = 0;
        for (Triplet<OperatorName, FunctionName, Integer> objects : operation) {
            if (Objects.equals(objects.getValue0(), OperatorName.CHANGE_KEY)) {
                result ++;
            }
        }

        return result;
    }
}



/*
   Operation:
       MAP: map;function;quantity
       FILTER: filter;function;quantity
       CHANGE KEY: change_key;function;quantity
       REDUCE: reduce;function;quantity

   Function:
       map, change key, reduce:
            add, sub, mult, div
       filter:
            equal, not_equal, greater_than, greater_or_equal, lower_than, lower_or_equal
 */