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

import com.google.protobuf.ByteString;

import java.util.*;

public class ManageDAG {
    /**
     * Number of Task Manager in the directed acyclic graph.
     */
    private int numberOfTaskManager = 0;

    private Vector<Long> freeTaskManagers = new Vector<>();

    /**
     * Total number of Task.
     */
    private int numberOfTask;

    private int maxTasksPerTaskManger = WorkerManager.TASK_SLOTS;
    private int maxTasksPerGroup;

    /**
     * Map between the TaskManagerID and the number of Task per TaskManager.
     */
    private HashMap<Long, Integer> taskPerTaskManager = new HashMap<>();

    /**
     * Map between the TaskID and the TaskManagerID.
     */
    private HashMap<Long, Long> taskIsInTaskManager = new HashMap<>();

    /**
     * Map of groups and TaskIDs are assigned for each group.
     */
    private HashMap<Long, HashSet<Long>> tasksInGroup = new HashMap<>();

    /**
     * Map the actual group and its follower group.
     * Value = -1 if and only if the follower is the coordinator.
     */
    private HashMap<Long, Long> followerGroup = new HashMap<>();

    /**
     * Set of checkpoints group.
     */
    private HashSet<Long> checkPoints = new HashSet<>();

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

        long taskManagerID = 0;
        taskIsInTaskManager.put(0L, taskManagerID);
        for (long i = 1L; i < (long) maxTasksPerTaskManger * numberOftasksManagers; i++) {
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
    public HashMap<Long, Integer> getTaskPerTaskManager() {
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
    public HashMap<Long, Long> getTaskIsInTaskManager() {
        return taskIsInTaskManager;
    }

    /**
     * Getter --> gets the map of all group and Task per group.
     *
     * @return the map of all group and Task per group.
     */
    public HashMap<Long, HashSet<Long>> getTasksInGroup() {
        return tasksInGroup;
    }

    public HashSet<Long> getTasksOfGroup(long groupId) {
        return tasksInGroup.get(groupId);
    }

    /**
     * Getter --> gets the set of checkpoints group.
     *
     * @return the set of checkpoints group.
     */
    public HashSet<Long> getCheckPoints() {
        return checkPoints;
    }

    /**
     * Getter --> gets the followers of the groups.
     *
     * @return the followers of the groups.
     */
    public HashMap<Long, Long> getFollowerGroup() {
        return followerGroup;
    }

    /**
     * Setter --> sets the number of Task Manager in the directed acyclic graph.
     *
     * @param numberOfTM the number of Task Manager in the directed acyclic
     *                   graph.
     */
    public void setNumberOfTaskManager(int numberOfTM) {
        int oldNumberOfTaskManager = this.numberOfTaskManager;

        this.numberOfTaskManager = numberOfTM;

        if (oldNumberOfTaskManager < numberOfTM) {
            for (long i = oldNumberOfTaskManager; i < numberOfTM; i++) {
                freeTaskManagers.add(i);
            }
        }
    }

    public void addFreeTaskManager(long taskManagerId) {
        if (numberOfTaskManager <= taskManagerId) {
            throw new NoSuchElementException("Task Manager ID is not valid");
        }

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
    public void setTaskPerTaskManager(HashMap<Long, Integer> taskPerTaskManager) {
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
    public void setTaskIsInTaskManager(HashMap<Long, Long> taskIsInTaskManager) {
        this.taskIsInTaskManager = taskIsInTaskManager;
    }

    /**
     * Setter --> sets the map of all group and Task per group.
     *
     * @param tasksInGroup the map of all group and Task per group.
     */
    public void setTasksInGroup(HashMap<Long, HashSet<Long>> tasksInGroup) {
        this.tasksInGroup = tasksInGroup;
    }

    /**
     * Setter --> sets the set of checkpoints group.
     *
     * @param checkPoints the set of checkpoints group.
     */
    public void setCheckPoints(HashSet<Long> checkPoints) {
        this.checkPoints = checkPoints;
    }

    /**
     * Setter --> sets the followers of the groups.
     *
     * @param followerGroup the followers of the groups.
     */
    public void setFollowerGroup(HashMap<Long, Long> followerGroup) {
        this.followerGroup = followerGroup;
    }

    /**
     * Adds a new Task Manager.
     *
     * @param taskManager the Task Manager to add.
     */
    public void addTaskManager(Pair<Long, Integer> taskManager) {
        this.taskPerTaskManager.put(taskManager.getValue0(), taskManager.getValue1());
        this.numberOfTask += taskManager.getValue1();
        this.numberOfTaskManager++;
    }

    /**
     * Removes a Task Manager.
     *
     * @param taskManager the Task Manager ID.
     */
    public void removeTaskManager(Long taskManager) {
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
    public void addTask(Long taskManager, Integer taskToAdd) {
        this.taskPerTaskManager.replace(taskManager, this.taskPerTaskManager.get(taskManager) + taskToAdd);
        this.numberOfTask += taskToAdd;
    }

    /**
     * Adds a new Task in a specific Task Manager.
     *
     * @param taskManagerId the Task Manager ID.
     * @param taskId        the Task ID.
     */
    public void addTask(Long taskManagerId, long taskId) {
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
    public void removeTask(Long taskManager, Integer taskToRemove) {
        this.taskPerTaskManager.replace(taskManager, this.taskPerTaskManager.get(taskManager) - taskToRemove);
        this.numberOfTask -= taskToRemove;
    }

    /**
     * Removes a Task in a specific Task Manager.
     *
     * @param taskManagerId the Task Manager ID.
     * @param taskId        the Task ID.
     */
    public void removeTask(Long taskManagerId, long taskId) {
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

        /// Only if there is some left-over operation, then make a new group. As an
        /// exception, if there are no operations, then make a new group.
        if (op.size() > 0 || operationsGroup.size() == 0) {
            operationsGroup.add(op);
        }

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
        // Set of tasks in the group.
        HashSet<Long> task = new HashSet<>();
        // Assignment of tasks to a group
        HashMap<Long, HashSet<Long>> tg = new HashMap<>();
        // Group id.
        long gid = 0;
        // Follower group
        HashMap<Long, Long> nextGroup = new HashMap<>();

        // assign tasks to group id
        task.add(0L);
        for (long i = 1L; i < this.numberOfTask; i++) {
            if ((task.size() % this.maxTasksPerGroup) == 0) {
                tg.put(gid, task);
                nextGroup.put(gid, gid + 1);
                gid++;
                task = new HashSet<>();
            }
            task.add(i);
        }
        if (tg.size() < getNumberOfGroups()) {
            tg.put(gid, task);
        }

        nextGroup.put(gid - 1, -1L);

        // set tasks to a group
        this.setTasksInGroup(tg);

        // set the next group to pass data to
        this.setFollowerGroup(nextGroup);

        //sets the effective number of tasks used
        this.setNumberOfTask(this.getNumberOfGroups() * this.maxTasksPerGroup);
    }

    /**
     * Assigns which groups are checkpoints.
     *
     * @param numberOfCheckpoint the number of checkpoints requested.
     */
    public void assignCheckpoint(int numberOfCheckpoint) {
        // Checkpoints.
        HashSet<Long> cp = new HashSet<>();

        if (numberOfCheckpoint > this.operationsGroup.size()) {
            // assigns each group as a checkpoint
            cp.addAll(tasksInGroup.keySet());
        } else {
            // Index.
            int i = 0;
            for (Long groupID : tasksInGroup.keySet()) {
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

    public Optional<Long> getNextFreeTaskManager() {
        if (freeTaskManagers.size() > 0) {
            return Optional.of(freeTaskManagers.remove(freeTaskManagers.size() - 1));
        }

        return Optional.empty();
    }

    public Optional<Long> groupFromTask(long taskId) {
        for (Long group : tasksInGroup.keySet()) {
            if (tasksInGroup.get(group).contains(taskId)) {
                return Optional.of(group);
            }
        }

        return Optional.empty();
    }

    public List<Long> getTasksOfTaskManager(long taskManagerId) {
        return taskIsInTaskManager
                .keySet()
                .stream()
                .filter(taskId -> taskIsInTaskManager.get(taskId) == taskManagerId)
                .map(Long::valueOf)
                .toList();
    }

    public Set<Long> getManagersOfNextGroup(long group_id) {
        var nextTasks = tasksInGroup.get(group_id + 1)
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
    public Set<Long> getTaskInTaskManager(long taskManager) {
        Set<Long> result = new HashSet<>();

        for (Map.Entry<Long, Long> entry : taskIsInTaskManager.entrySet()) {
            if (entry.getValue() == taskManager) {
                result.add(entry.getKey());
            }
        }

        return result;
    }

    /**
     * Retrieves the list of operations assigned to a specific task manager.
     *
     * @param taskManagerId the task manager.
     * @return the list of operations to be performed by the tasks managed by a
     *         worker manager according to their group membership.
     */
    public List<Pair<List<Triplet<OperatorName, FunctionName, Integer>>, Long>> getOperationsForTaskManager(
            long taskManagerId) {
        List<Pair<List<Triplet<OperatorName, FunctionName, Integer>>, Long>> operations = new ArrayList<>();

        // Get the tasks of the task manager
        List<Long> tasks = getTasksOfTaskManager((int) taskManagerId);
        // Get the groups of the task
        List<Long> groups = getGroupsOfTaskManager(taskManagerId);
        // Get the operations of the group
        for (Long group : groups) {
            operations.add(new Pair<>(operationsGroup.get((int) (long) group), group));
        }

        return operations;
    }

    /**
     * Gets groups where there are tasks that are managed by a task manager.
     *
     * @param taskManagerId the task manager to find.
     * @return the groups where there are tasks that are managed by the task manager
     */
    private List<Long> getGroupsOfTaskManager(long taskManagerId) {
        List<Long> result = new ArrayList<>();

        // Find all tasks in a task manager.
        List<Long> matchingTid = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : taskIsInTaskManager.entrySet()) {
            if (entry.getValue() == taskManagerId) {
                matchingTid.add(entry.getKey());
            }
        }

        // find all group that contains
        for (Map.Entry<Long, HashSet<Long>> entry : tasksInGroup.entrySet()) {
            HashSet<Long> tIds = entry.getValue();

            for (Long id : matchingTid) {
                if (tIds.contains(id)) {
                    result.add(entry.getKey());
                    break;
                }
            }
        }

        return result;
    }

    /**
     * Saves the current state of data and operations at a specific checkpoint.
     *
     * @param checkpointRequest the request object containing the group ID, data,
     *                          and list of operations to be saved.
     */
    public void saveCheckpoint(CheckpointRequest checkpointRequest) {
        if (this.lastCheckpoint.getValue0() != checkpointRequest.getGroupId()) {
            // Save the data.
            List<Pair<Integer, Integer>> dataCheckpoint = ManageCSVfile.readCSVinput(checkpointRequest.getDataList());
            this.lastCheckpoint = new Pair<>(checkpointRequest.getGroupId(), dataCheckpoint);
        } else {
            // Add the data.
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
