package it.polimi.ds.Directed_Acyclic_Graph;

import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.HashMap;
import java.util.List;

public class ManageDAG {
    /**
     * Number of Task Manager in the directed acyclic graph.
     */
    private int numberOfTaskManager;
    /**
     * Total number of Task.
     */
    private int numberOfTask;
    /**
     * Map between the TaskManagerID and the number of Task per TaskManager.
     */
    private HashMap<Integer, Integer> taskPerTaskManager;



    /**
     * Constructor --> initialize the ManageDAG.
     *
     * @param taskPerTaskManager the map between the TaskManagerID and the number of Task per TaskManager.
     */
    public ManageDAG(HashMap<Integer, Integer> taskPerTaskManager) {
        this.taskPerTaskManager = taskPerTaskManager;

        this.numberOfTaskManager = taskPerTaskManager.size();

        this.numberOfTask = 0;
        for (HashMap.Entry<Integer, Integer> entry : taskPerTaskManager.entrySet()) {
            numberOfTask += entry.getValue();
        }
    }



    /**
     * Getter --> gets the number of Task Manager in the directed acyclic graph.
     *
     * @return the number of Task Manager in the directed acyclic graph.
     */
    public int getNumberOfTaskManager() {
        return numberOfTaskManager;
    }

    /**
     * Getter --> gets the total number of task in the directed acyclic graph.
     *
     * @return the total number of task in the directed acyclic graph.
     */
    public int getNumberOfTask() {
        return numberOfTask;
    }

    /**
     * Getter --> gets the map between the TaskManagerID and the number of Task per TaskManager.
     *
     * @return the map between the TaskManagerID and the number of Task per TaskManager.
     */
    public HashMap<Integer, Integer> getTaskPerTaskManager() {
        return taskPerTaskManager;
    }



    /**
     * Setter --> sets the number of Task Manager in the directed acyclic graph.
     *
      * @param numberOfTaskManager the number of Task Manager in the directed acyclic graph.
     */
    public void setNumberOfTaskManager(int numberOfTaskManager) {
        this.numberOfTaskManager = numberOfTaskManager;
    }

    /**
     * Setter --> sets the total number of task in the directed acyclic graph.
     *
     * @param numberOfTask the total number of task in the directed acyclic graph.
     */
    public void setNumberOfTask(int numberOfTask) {
        this.numberOfTask = numberOfTask;
    }

    /**
     * Setter --> sets the map between the TaskManagerID and the number of Task per TaskManager.
     *
     * @param taskPerTaskManager the map between the TaskManagerID and the number of Task per TaskManager.
     */
    public void setTaskPerTaskManager(HashMap<Integer, Integer> taskPerTaskManager) {
        this.taskPerTaskManager = taskPerTaskManager;
    }



    /**
     * Adds a new Task Manager.
     *
     * @param taskManager the Task Manager to add.
     */
    public void addTaskManager(Pair<Integer, Integer> taskManager) {
        this.taskPerTaskManager.put(taskManager.getValue0(), taskManager.getValue1());
        this.numberOfTask += taskManager.getValue1();
        this.numberOfTaskManager++;
    }

    /**
     * Removes a Task Manager.
     *
     * @param taskManager the Task Manager ID.
     */
    public void removeTaskManager(Integer taskManager) {
        this.numberOfTask -= this.taskPerTaskManager.get(taskManager);
        this.taskPerTaskManager.remove(taskManager);
        this.numberOfTaskManager--;
    }


    /**
     * Adds a new Task in a specific Task Manager.
     *
     * @param taskManager the Task Manager ID.
     * @param taskToAdd the number of Tasks to add.
     */
    public void addTask(Integer taskManager, Integer taskToAdd) {
        this.taskPerTaskManager.replace(taskManager, this.taskPerTaskManager.get(taskManager) + taskToAdd);
        this.numberOfTask += taskToAdd;
    }

    /**
     * Removes a Task in a specific Task Manager.
     *
     * @param taskManager the Task Manager ID.
     * @param taskToRemove the number of Tasks to remove.
     */
    public void removeTask(Integer taskManager, Integer taskToRemove) {
        this.taskPerTaskManager.replace(taskManager, this.taskPerTaskManager.get(taskManager) - taskToRemove);
        this.numberOfTask -= taskToRemove;
    }



    /**
     * Returns the number of tasks required for each computation block before a key change operation.
     *
     * @param operation the list of all operation to compute.
     * @return the number of tasks required for each computation block before a key change operation.
     */
    public Integer modulo(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        return (Integer) (numberOfTask / Operator.numberOfChangeKeys(operation));
    }
}
