package it.polimi.ds;

public class WorkerManager {

    public static final int TASK_SLOTS = 5;

    private Task[] tasks = new Task[TASK_SLOTS];

    public static void main(String[] args) {
        System.out.println(Address.fromString(args[0]).getValue0());
    }

    class Task {

    }
}
