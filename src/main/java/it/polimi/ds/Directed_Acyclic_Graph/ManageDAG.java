package it.polimi.ds.Directed_Acyclic_Graph;

import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    private HashMap<Integer, Integer> taskPerTaskManager = new HashMap<>();

    /**
     * Map between the TaskID and the TaskManagerID.
     */
    private HashMap<Integer, Integer> taskIsInTaskManager = new HashMap<>();

    /**
     * Number of operation group.
     */
    private int numberOfGroups;

    /**
     * Map of groups and TaskIDs are assigned for each group.
     */
    private HashMap<Integer, HashSet<Integer>> tasksInGroup = new HashMap<>();

    /**
     * Map the actual group and its follower group.
     * Value = -1 if and only if the follower is the coordinator.
     */
    private HashMap<Integer, Integer> followerGroup  = new HashMap<>();

    /**
     * Set of checkpoints group.
     */
    private HashSet<Integer> checkPoints = new HashSet<>();

    /**
     * List of operations to be calculated in a single operation group.
     * A group of operations consists of operations until a key change operation occurs.
     */
    private ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup = new ArrayList<>();

    /**
     * Data to be computed.
     */
    private List<Pair<Integer, Integer>> data = new ArrayList<>();


    /**
     * Constructor
     */
    public ManageDAG() {
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
     * Getter --> gets the list of operations to be calculated in a single operation group.
     *
     * @return the list of operations to be calculated in a single operation group.
     */
    public ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> getOperationsGroup() {
        return operationsGroup;
    }

    /**
     * Getter --> gets the list of all data to compute.
     *
     * @return the list of all data to compute.
     */
    public List<Pair<Integer, Integer>> getData() {
        return data;
    }

    /**
     * Getter --> gets the number of group of operation needed.
     *
     * @return the number of group of operation needed.
     */
    public int getNumberOfGroups() {
        return numberOfGroups;
    }

    /**
     * Getter --> gets the map between the TaskID and the TaskManagerID.
     *
     * @return the map between the TaskID and the TaskManagerID.
     */
    public HashMap<Integer, Integer> getTaskIsInTaskManager() {
        return taskIsInTaskManager;
    }

    /**
     * Getter --> gets the map of all group and Task per group.
     *
     * @return the map of all group and Task per group.
     */
    public HashMap<Integer, HashSet<Integer>> getTasksInGroup() {
        return tasksInGroup;
    }

    /**
     * Getter --> gets the set of checkpoints group.
     *
     * @return the set of checkpoints group.
     */
    public HashSet<Integer> getCheckPoints() {
        return checkPoints;
    }

    /**
     * Getter --> gets the followers of the groups.
     *
     * @return the followers of the groups.
     */
    public HashMap<Integer, Integer> getFollowerGroup() {
        return followerGroup;
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
     * Setter --> sets the list of operations to be calculated in a single operation group.
     *
     * @param operationsGroup the list of operations to be calculated in a single operation group.
     */
    public void setOperationsGroup(ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup) {
        this.operationsGroup = operationsGroup;
        this.numberOfGroups = operationsGroup.size();
    }

    /**
     * Setter --> sets the list of all data to compute.
     *
     * @param data the list of all data to compute.
     */
    public void setData(List<Pair<Integer, Integer>> data) {
        this.data = data;
    }

    /**
     * Setter --> sets the number of group of operation needed.
     *
     * @param numberOfGroups the number of group of operation needed.
     */
    public void setNumberOfGroups(int numberOfGroups) {
        this.numberOfGroups = numberOfGroups;
    }

    /**
     * Setter --> sets the map between the TaskID and the TaskManagerID.
     *
     * @param taskIsInTaskManager the map between the TaskID and the TaskManagerID.
     */
    public void setTaskIsInTaskManager(HashMap<Integer, Integer> taskIsInTaskManager) {
        this.taskIsInTaskManager = taskIsInTaskManager;
    }

    /**
     * Setter --> sets the map of all group and Task per group.
     *
     * @param tasksInGroup the map of all group and Task per group.
     */
    public void setTasksInGroup(HashMap<Integer, HashSet<Integer>> tasksInGroup) {
        this.tasksInGroup = tasksInGroup;
    }

    /**
     * Setter --> sets the set of checkpoints group.
     *
     * @param checkPoints the set of checkpoints group.
     */
    public void setCheckPoints(HashSet<Integer> checkPoints) {
        this.checkPoints = checkPoints;
    }

    /**
     * Setter --> sets the followers of the groups.
     *
     * @param followerGroup the followers of the groups.
     */
    public void setFollowerGroup(HashMap<Integer, Integer> followerGroup) {
        this.followerGroup = followerGroup;
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
     * Adds a new Task in a specific Task Manager.
     *
     * @param taskManagerId the Task Manager ID.
     * @param taskId the Task ID.
     */
    public void addTask(Integer taskManagerId, int taskId) {
        this.taskPerTaskManager.replace(taskManagerId, this.taskPerTaskManager.get(taskManagerId) + 1);
        this.numberOfTask++;
        this.taskIsInTaskManager.put(taskId, taskManagerId);
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
     * Removes a Task in a specific Task Manager.
     *
     * @param taskManagerId the Task Manager ID.
     * @param taskId the Task ID.
     */
    public void removeTask(Integer taskManagerId, int taskId) {
        this.taskPerTaskManager.replace(taskManagerId, this.taskPerTaskManager.get(taskManagerId) - 1);
        this.numberOfTask--;
        this.taskIsInTaskManager.remove(taskId);
    }


    /**
     * Adds an operation group.
     *
     * @param operation the operation group.
     */
    public void addOperationGroup(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        this.operationsGroup.add(operation);
    }

    /**
     * Removes an operation group.
     *
     * @param operation the operation group.
     */
    public void removeOperationGroup(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        this.operationsGroup.remove(operation);
    }

    /**
     * Generates groups of operations that end with either a Change Key operation or a Reduce operation.
     *
     * @param operations the list of all operation to compute.
     */
    public void generateOperationsGroup(List<Triplet<OperatorName, FunctionName, Integer>> operations) {
        //list of operation group
        ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup = new ArrayList<>();
        //group of operation
        List<Triplet<OperatorName, FunctionName, Integer>> op = new ArrayList<>();

        for (Triplet<OperatorName, FunctionName, Integer> operation : operations) {
            op.add(operation);
            if (operation.getValue0() == OperatorName.CHANGE_KEY || operation.getValue0() == OperatorName.REDUCE) {
                operationsGroup.add(op);
                op = new ArrayList<>();
            }
        }

        //sets operation groups & the number of group needed.
        setOperationsGroup(operationsGroup);
    }


    /**
     * Returns the number of tasks required for each computation block before a key change operation.
     *
     * @param operation the list of all operation to compute.
     * @return the number of tasks required for each computation block before a key change operation.
     */
    public Integer numberIdPerTask(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        return (Integer) (numberOfTask / Operator.numberOfChangeKeys(operation));
    }

    /**
     * Divides tasks into groups where each group performs a part of the overall computation.
     */
    public void divideTaskInGroup() {
        //Number of task per group
        int taskPerGroup = (int) numberOfGroups / numberOfTask;
        //Tasks to be added to the group.
        HashSet<Integer> tasks = new HashSet<>();
        //Task in all the group.
        HashMap<Integer, HashSet<Integer>> taskInGroup = new HashMap<>();
        //Group id.
        int groupID = 0;
        //Follower group
        HashMap<Integer, Integer> nextGroup = new HashMap<>();

        for (Integer task : taskIsInTaskManager.keySet()) {
            //create group: group id, task set
            tasks.add(task);
            if (tasks.size() == taskPerGroup) {
                taskInGroup.put(groupID, tasks);
                tasks = new HashSet<>();
                groupID++;
            }

            //create the map between the actual group id and the next one - -1 if the successor is the coordinator because the computation is finished
            nextGroup.put(groupID, groupID != numberOfGroups - 1 ? groupID + 1 : -1);
        }

        //set the map group id, task set
        this.setTasksInGroup(taskInGroup);

        //set the group follower
        this.setFollowerGroup(nextGroup);
    }

    /**
     * Assigns which groups are checkpoints.
     *
     * @param numberOfCheckpoint the number of checkpoints requested.
     */
    public void assignCheckpoint(int numberOfCheckpoint) {
        //Checkpoints.
        HashSet<Integer> cp = new HashSet<>();

        if (numberOfCheckpoint > numberOfGroups) {
            //assigns each group as a checkpoint
            cp.addAll(tasksInGroup.keySet());
        }
        else {
            //Index.
            int i = 0;
            for (Integer groupID : tasksInGroup.keySet()) {
                //assigns as checkpoints only checkpoints that are multiple of the required number of checkpoints
                if (i % (this.numberOfGroups / numberOfCheckpoint) == 0) {
                    cp.add(groupID);
                }
                i++;
            }
        }

        //sets the checkpoints
        this.setCheckPoints(cp);
    }


    /*
        insieme 1 --> insieme 2 -->



        DAG:
        - insiemi successivi
            - TMid
            - Tid
        - check point

        operation to execute
        last data
        a chi mando i dati
     */
}
