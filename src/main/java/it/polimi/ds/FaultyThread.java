package it.polimi.ds;

import java.util.Random;

import io.github.cdimascio.dotenv.Dotenv;

public class FaultyThread extends Thread {
    private static final Dotenv dotenv = Dotenv.load();
    public static final int SLEEP_TIME = dotenv.get("FAULTY_THREADS_SECS_INTERVAL") != null
            ? Integer.parseInt(dotenv.get("FAULTY_THREADS_SECS_INTERVAL")) * 1000
            : 10_000;

    public static final float FAULT_PROBABILITY = dotenv.get("FAULT_PROBABILITY") != null
            ? Float.parseFloat(dotenv.get("FAULT_PROBABILITY"))
            : 0.05f;

    private Random random = new Random();

    @Override
    public void run() {
        if (dotenv.get("FAULTY_THREADS") != null &&
                dotenv.get("FAULTY_THREADS").toLowerCase().equals("true")) {
            System.err.println("\u001B[33mStarting faulty thread, good luck!\u001B[0m");
            while (true) {
                try {
                    Thread.sleep(SLEEP_TIME);
                    double randomInt = random.nextDouble();
                    if (randomInt < FAULT_PROBABILITY) {
                        System.err.println("Simulating a fault in the thread");
                        System.exit(1);
                    }
                } catch (Exception e) {
                    System.err.println("Simulating a fault in the thread");
                    System.exit(1);
                }
            }
        }
    }
}
