package it.polimi.ds.function;


/**
 * The OperatorName class is an enumeration that represents different types of operators used in a data processing system.
 */
public enum OperatorName {
    /**
     * Represents the mapping operator.
     */
    MAP,
    /**
     * Represents the filtering operator.
     */
    FILTER,
    /**
     * Represents the operator for changing keys.
     */
    CHANGE_KEY,
    /**
     * Represents the reducing operator.
     */
    REDUCE;


    /**
     * Gets the value of OperatorName enumeration.
     *
     * @param string the input string.
     * @return the enumeration value that corresponds with the input string.
     */
    public static OperatorName getsEnumerationValue(String string) {
        return switch (string.toLowerCase()) {
            case "map" -> MAP;
            case "filter" -> FILTER;
            case "change_key" -> CHANGE_KEY;
            case "reduce" -> REDUCE;
            default -> null;
        };
    }
}
