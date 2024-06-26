package it.polimi.ds.function;


/**
 * The FunctionName class is an enumeration that represents different mathematical operations such as addition, subtraction, multiplication, and division.
 */
public enum FunctionName {
    /**
     * Represents the addition operation.
     */
    ADD,
    /**
     * Represents the subtraction operation.
     */
    SUB,
    /**
     * Represents the multiplication operation.
     */
    MULT,
    /**
     * Represents the division operation.
     */
    DIV,
    /**
     * Represents the equality comparison operator.
     */
    EQUAL,
    /**
     * Represents the inequality comparison operator.
     */
    NOT_EQUAL,
    /**
     * Represents the greater than comparison operator.
     */
    GREATER_THAN,
    /**
     * Represents the greater than or equal to comparison operator.
     */
    GREATER_OR_EQUAL,
    /**
     * Represents the lower than comparison operator.
     */
    LOWER_THAN,
    /**
     * Represents the lower than or equal to comparison operator.
     */
    LOWER_OR_EQUAL;


    /**
     * Gets the value of FunctionName enumeration.
     *
     * @param string the input string.
     * @return the enumeration value that corresponds with the input string.
     */
    public static FunctionName getsEnumerationValue(String string) {
        return switch (string.toLowerCase()) {
            case "add" -> ADD;
            case "sub" -> SUB;
            case "mult" -> MULT;
            case "div" -> DIV;
            case "equal" -> EQUAL;
            case "not_equal" -> NOT_EQUAL;
            case "greater_than" -> GREATER_THAN;
            case "greater_of_equal" -> GREATER_OR_EQUAL;
            case "lower_than" -> LOWER_THAN;
            case "lower_or_equal" -> LOWER_OR_EQUAL;
            default -> null;
        };
    }
}
