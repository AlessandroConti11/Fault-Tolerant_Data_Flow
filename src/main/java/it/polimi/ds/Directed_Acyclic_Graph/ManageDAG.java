package it.polimi.ds.Directed_Acyclic_Graph;

import it.polimi.ds.Exceptions;
import it.polimi.ds.WorkerManager;
import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.CheckpointRequest;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Tuple;

import com.google.protobuf.ByteString;

import java.util.*;

public class ManageDAG {
    /**
     * Number of Task Manager in the directed acyclic graph.
     */
    private int numberOfTaskManager = 0;

    private Vector<Integer> freeTaskManagers = new Vector<>();

    /**
     * Total number of Task.
     */
    private int numberOfTask;

    private int maxTasksPerTaskManger = WorkerManager.TASK_SLOTS;
    private int maxTasksPerGroup;

    /**
     * Map between the TaskManagerID and the number of Task per TaskManager.
     */
    private HashMap<Integer, Integer> taskPerTaskManager = new HashMap<>();

    /**
     * Map between the TaskID and the TaskManagerID.
     */
    private HashMap<Integer, Integer> taskIsInTaskManager = new HashMap<>();

    /**
     * Map of groups and TaskIDs are assigned for each group.
     */
    private HashMap<Integer, HashSet<Integer>> tasksInGroup = new HashMap<>();

    /**
     * Map the actual group and its follower group.
     * Value = -1 if and only if the follower is the coordinator.
     */
    private HashMap<Integer, Integer> followerGroup = new HashMap<>();

    /**
     * Set of checkpoints group.
     */
    private HashSet<Integer> checkPoints = new HashSet<>();

    /**
     * List of operations to be calculated in a single operation group.
     * A group of operations consists of operations until a key change operation
     * occurs.
     */
    private ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup = new ArrayList<>();

    /**
     * Data to be computed.
     */
    private List<Pair<Integer, Integer>> data = new ArrayList<>();

    /**
     * Last checkpoint - group id, data, operation.
     */
    private Pair<Long, List<Pair<Integer, Integer>>> lastCheckpoint;


    /**
     * Constructor
     */
    public ManageDAG(ByteString program, int numberOftasksManagers) throws Exception {
        if (!ManageCSVfile.checkCSVoperator(program)) {
            throw new Exceptions.MalformedProgramFormatException();
        }

        this.setNumberOfTaskManager(numberOftasksManagers);

        // divide operation into subgroups ending with a Change Key & define the number
        // of operation group needed.
        this.generateOperationsGroup(ManageCSVfile.readCSVoperation(program));

        this.setNumberOfTask(numberOftasksManagers * maxTasksPerTaskManger);

        this.maxTasksPerGroup = (getNumberOfTaskManager() * maxTasksPerTaskManger) / this.getNumberOfGroups();
        if (this.maxTasksPerGroup == 0) {
            throw new Exceptions.NotEnoughResourcesException();
        }

        int taskManagerID = 0;
        taskIsInTaskManager.put(0, taskManagerID);
        for (int i = 1; i < maxTasksPerTaskManger * numberOftasksManagers; i++) {
            taskIsInTaskManager.put(i, (i + 1) % maxTasksPerTaskManger == 0 ? taskManagerID++ : taskManagerID);
        }

        // divide the task in group & assign the group order
        this.divideTaskInGroup();

        // TODO: how to decide how many checkpoints are needed
        this.assignCheckpoint(2);
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
     * Getter --> gets the map between the TaskManagerID and the number of Task per
     * TaskManager.
     *
     * @return the map between the TaskManagerID and the number of Task per
     *         TaskManager.
     */
    public HashMap<Integer, Integer> getTaskPerTaskManager() {
        return taskPerTaskManager;
    }

    /**
     * Getter --> gets the list of operations to be calculated in a single operation
     * group.
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
        return this.operationsGroup.size();
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

    public HashSet<Integer> getTasksOfGroup(int groupId) {
        return tasksInGroup.get(groupId);
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
     * @param numberOfTM the number of Task Manager in the directed acyclic
     *                            graph.
     */
    public void setNumberOfTaskManager(int numberOfTM) {
        int oldNumberOfTaskManager = this.numberOfTaskManager;

        this.numberOfTaskManager = numberOfTM;

        if (oldNumberOfTaskManager < numberOfTM) {
            for (int i = oldNumberOfTaskManager; i < numberOfTM; i++) {
                freeTaskManagers.add(i);
            }
        }
    }

    public void addFreeTaskManager(int taskManagerId) {
        freeTaskManagers.add(taskManagerId);
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
     * Setter --> sets the map between the TaskManagerID and the number of Task per
     * TaskManager.
     *
     * @param taskPerTaskManager the map between the TaskManagerID and the number of
     *                           Task per TaskManager.
     */
    public void setTaskPerTaskManager(HashMap<Integer, Integer> taskPerTaskManager) {
        this.taskPerTaskManager = taskPerTaskManager;
    }

    /**
     * Setter --> sets the list of operations to be calculated in a single operation
     * group.
     *
     * @param operationsGroup the list of operations to be calculated in a single
     *                        operation group.
     */
    public void setOperationsGroup(ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup) {
        this.operationsGroup = operationsGroup;
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
     * @param taskToAdd   the number of Tasks to add.
     */
    public void addTask(Integer taskManager, Integer taskToAdd) {
        this.taskPerTaskManager.replace(taskManager, this.taskPerTaskManager.get(taskManager) + taskToAdd);
        this.numberOfTask += taskToAdd;
    }

    /**
     * Adds a new Task in a specific Task Manager.
     *
     * @param taskManagerId the Task Manager ID.
     * @param taskId        the Task ID.
     */
    public void addTask(Integer taskManagerId, int taskId) {
        this.taskPerTaskManager.replace(taskManagerId, this.taskPerTaskManager.get(taskManagerId) + 1);
        this.numberOfTask++;
        this.taskIsInTaskManager.put(taskId, taskManagerId);
    }

    /**
     * Removes a Task in a specific Task Manager.
     *
     * @param taskManager  the Task Manager ID.
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
     * @param taskId        the Task ID.
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
     * Generates groups of operations that end with either a Change Key operation or
     * a Reduce operation.
     *
     * @param operations the list of all operation to compute.
     */
    public void generateOperationsGroup(List<Triplet<OperatorName, FunctionName, Integer>> operations) {
        // list of operation group
        ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup = new ArrayList<>();
        // group of operation
        List<Triplet<OperatorName, FunctionName, Integer>> op = new ArrayList<>();

        for (Triplet<OperatorName, FunctionName, Integer> operation : operations) {
            op.add(operation);
            if (operation.getValue0() == OperatorName.CHANGE_KEY || operation.getValue0() == OperatorName.REDUCE) {
                operationsGroup.add(op);
                op = new ArrayList<>();
            }
        }
        operationsGroup.add(op);

        // sets operation groups & the number of group needed.
        setOperationsGroup(operationsGroup);
    }

    /**
     * Returns the number of tasks required for each computation block before a key
     * change operation.
     *
     * @param operation the list of all operation to compute.
     * @return the number of tasks required for each computation block before a key
     *         change operation.
     */
    public Integer numberIdPerTask(List<Triplet<OperatorName, FunctionName, Integer>> operation) {
        return (Integer) (numberOfTask / Operator.numberOfChangeKeys(operation));
    }

    /**
     * Divides tasks into groups where each group performs a part of the overall
     * computation.
     */
    public void divideTaskInGroup() {
        //Set of tasks in the group.
        HashSet<Integer> task = new HashSet<>();
        //Assignment of tasks to a group
        HashMap<Integer, HashSet<Integer>> tg = new HashMap<>();
        //Group id.
        int gid = 0;
        //Follower group
        HashMap<Integer, Integer> nextGroup = new HashMap<>();

        //assign tasks to group id
        task.add(0);
        for (int i = 1; i < this.numberOfTask; i++) {
            if ((task.size() % this.maxTasksPerGroup) == 0){
                tg.put(gid, task);
                nextGroup.put(gid, gid + 1);
                gid++;
                task = new HashSet<>();
            }
            task.add(i);
        }
        nextGroup.put(gid - 1, -1);

        //set tasks to a group
        this.setTasksInGroup(tg);

        //set the next group to pass data to
        this.setFollowerGroup(nextGroup);
    }

    /**
     * Assigns which groups are checkpoints.
     *
     * @param numberOfCheckpoint the number of checkpoints requested.
     */
    public void assignCheckpoint(int numberOfCheckpoint) {
        // Checkpoints.
        HashSet<Integer> cp = new HashSet<>();

        if (numberOfCheckpoint > this.operationsGroup.size()) {
            // assigns each group as a checkpoint
            cp.addAll(tasksInGroup.keySet());
        } else {
            // Index.
            int i = 0;
            for (Integer groupID : tasksInGroup.keySet()) {
                // assigns as checkpoints only checkpoints that are multiple of the required
                // number of checkpoints
                if (i % (this.operationsGroup.size() / numberOfCheckpoint) == 0) {
                    cp.add(groupID);
                }
                i++;
            }
        }

        // sets the checkpoints
        this.setCheckPoints(cp);
    }

    public Optional<Integer> getNextFreeTaskManager() {
        if (freeTaskManagers.size() > 0) {
            return Optional.of(freeTaskManagers.remove(freeTaskManagers.size() - 1));
        }

        return Optional.empty();
    }

    public Optional<Long> groupFromTask(long taskId) {
        for (Integer group : tasksInGroup.keySet()) {
            if (tasksInGroup.get(group).contains((int) taskId)) {
                return Optional.of(Long.valueOf(group));
            }
        }

        return Optional.empty();
    }

    // TODO: Set Id to Long
    public List<Long> getTasksOfTaskManager(int taskManagerId) {
        return taskIsInTaskManager
                .keySet()
                .stream()
                .filter(taskId -> taskIsInTaskManager.get(taskId) == taskManagerId)
                .map(Long::valueOf)
                .toList();
    }

    // TODO: Set Id to Long
    public Set<Long> getManagersOfNextGroup(int group_id) {
        var nextTasks = tasksInGroup.get((int) (long) group_id + 1)
                .stream()
                .map(t -> taskIsInTaskManager.get(t))
                .map(Long::valueOf)
                .collect(java.util.stream.Collectors.toSet());

        Set<Long> managers = new HashSet<>();
        for (Long taskManagerId : nextTasks) {
            managers.add(taskManagerId);
        }

        return managers;
    }

    /**
     * Gets the task id that are managed by task manager.
     *
     * @param taskManager the task manager id.
     * @return the set of task id that are managed by the task manager.
     */
    public Set<Integer> getTaskInTaskManager(long taskManager) {
        Set<Integer> result = new HashSet<>();

        for (Map.Entry<Integer, Integer> entry : taskIsInTaskManager.entrySet()) {
            if (entry.getValue() == taskManager) {
                result.add(entry.getKey());
            }
        }

        return result;
    }

    public List<Pair<List<Triplet<OperatorName, FunctionName, Integer>>, Long>> getOperationsForTaskManager(long taskManagerId) {
        List<Pair<List<Triplet<OperatorName, FunctionName, Integer>>, Long>> operations = new ArrayList<>();

        // Get the tasks of the task manager
        List<Long> tasks = getTasksOfTaskManager((int) taskManagerId);
        // Get the group of the task
//        List<Long> groups = tasks.stream().map(this::groupFromTask).map(Optional::get).toList();
        List<Integer> groups = groupsThatHaveTaskManagedByTM(taskManagerId);
        // Get the operations of the group
//        for (Long group : groups) {
//            operations.add(new Pair<>(operationsGroup.get((int) (long) group), group));
//        }

        return operations;
    }

    public List<Integer> groupsThatHaveTaskManagedByTM(long taskManagerId) {
        List<Integer> result = new ArrayList<>();
        //Set of all task in the task manager.
        Set<Integer> task = getTaskInTaskManager(taskManagerId);

        for (Integer groupID : tasksInGroup.keySet()) {
            Set<Integer> value = tasksInGroup.get(groupID);
            for (Integer taskID : task) {
                if (value.contains(taskID)) {
                    result.add(groupID);
                }
            }
        }

        return result;
    }


    /**
     * Saves the current state of data and operations at a specific checkpoint.
     *
     * @param checkpointRequest the request object containing the group ID, data, and list of operations to be saved.
     */
    public void saveCheckpoint(CheckpointRequest checkpointRequest) {
        if (this.lastCheckpoint.getValue0() != checkpointRequest.getGroupId()) {
            //Save the data.
            List<Pair<Integer, Integer>> dataCheckpoint = ManageCSVfile.readCSVinput(checkpointRequest.getDataList());
            this.lastCheckpoint = new Pair<>(checkpointRequest.getGroupId(), dataCheckpoint);
        }
        else {
            //Add the data.
            List<Pair<Integer, Integer>> dataCheckpoint = this.lastCheckpoint.getValue1();
            dataCheckpoint.addAll(ManageCSVfile.readCSVinput(checkpointRequest.getDataList()));
            this.lastCheckpoint.setAt1(dataCheckpoint);
        }
    }



    /*
     * insieme 1 --> insieme 2 -->
     * 
     * 
     * 
     * DAG:
     * - insiemi successivi
     * - TMid
     * - Tid
     * - check point
     * 
     * operation to execute
     * last data
     * a chi mando i dati
     */
}
