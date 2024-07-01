package it.polimi.ds;

/**
 * Contains all the program exception.
 */
public class Exceptions {
    /**
     * Exception --> program file is NOT formed like Operation; Function; Quantity.
     */
    public static class MalformedProgramFormatException extends Exception {}

    /**
     * Exception --> insufficient computing resources
     */
    public static class NotEnoughResourcesException extends Exception {}
}
