package it.polimi.ds.function;

import static java.lang.Math.abs;

/**
 * The Function class provides a set of static methods that perform basic
 * mathematical operations such as addition, subtraction, multiplication, and
 * division.
 */
public class Function {
    /**
     * Add function.
     *
     * @param number the input number.
     * @param adder  the quantity to add from the number.
     * @return number + adder.
     */
    private static int add(int number, int adder) {
        return (number + adder);
    }

    /**
     * Subtract function.
     *
     * @param number   the input number.
     * @param subtract the quantity to subtract from the number.
     * @return number - subtract.
     */
    private static int sub(int number, int subtract) {
        return (number - subtract);
    }

    /**
     * Multiply function.
     *
     * @param number     the input number.
     * @param multiplier the quantity to multiply from the number.
     * @return number * multiplier.
     */
    private static int mult(int number, int multiplier) {
        return (number * multiplier);
    }

    /**
     * Division function.
     *
     * @param number  the input number.
     * @param divider the quantity to divide from the number.
     * @return number / divider.
     */
    private static int div(int number, int divider) {
        return (number / divider);
    }

    /**
     * Compares the number with the equal value.
     *
     * @param number the input number.
     * @param equal  the equality quantity.
     * @return 1 if the number is equal, 0 otherwise.
     */
    private static int equal(int number, int equal) {
        return number == equal ? 1 : 0;
    }

    /**
     * Compares the number with the notEqual value.
     *
     * @param number   the input number.
     * @param notEqual the not equality quantity.
     * @return 1 if the number is not equal, 0 otherwise.
     */
    private static int notEqual(int number, int notEqual) {
        return number != notEqual ? 1 : 0;
    }

    /**
     * Compares the number with the greater value.
     *
     * @param number  the input number.
     * @param greater the greater quantity.
     * @return 1 if the number is greater, 0 otherwise.
     */
    private static int greaterThan(int number, int greater) {
        return number > greater ? 1 : 0;
    }

    /**
     * Compares the number with the greaterEqual value.
     *
     * @param number       the input number.
     * @param greaterEqual the greater or equal quantity.
     * @return 1 if the number is greater or equal, 0 otherwise.
     */
    private static int greaterEqual(int number, int greaterEqual) {
        return number >= greaterEqual ? 1 : 0;
    }

    /**
     * Compares the number with the lower value.
     *
     * @param number the input number.
     * @param lower  the lower quantity.
     * @return 1 if the number is lower, 0 otherwise.
     */
    private static int lowerThan(int number, int lower) {
        return number < lower ? 1 : 0;
    }

    /**
     * Compares the number with the lowerEqual value.
     *
     * @param number     the input number.
     * @param lowerEqual the lower or equal quantity.
     * @return 1 if the number is lower or equal, 0 otherwise.
     */
    private static int lowerEqual(int number, int lowerEqual) {
        return number <= lowerEqual ? 1 : 0;
    }

    /**
     * Selects and executes one of the predefined functions.
     *
     * @param number   the input number.
     * @param quantity the quantity to add/subtract/multiply/divide from the number.
     * @param type     the default function type to call.
     * @return result = f(number).
     */
    public static int f(int number, int quantity, FunctionName type) {
        switch (type) {
            case ADD -> {
                return add(number, quantity);
            }
            case SUB -> {
                return sub(number, quantity);
            }
            case MULT -> {
                return mult(number, quantity);
            }
            case DIV -> {
                return div(number, quantity);
            }
            case EQUAL -> {
                return equal(number, quantity);
            }
            case NOT_EQUAL -> {
                return notEqual(number, quantity);
            }
            case GREATER_THAN -> {
                return greaterThan(number, quantity);
            }
            case GREATER_OR_EQUAL -> {
                return greaterEqual(number, quantity);
            }
            case LOWER_THAN -> {
                return lowerThan(number, quantity);
            }
            case LOWER_OR_EQUAL -> {
                return lowerEqual(number, quantity);
            }
        }

        return -1;
    }
}
