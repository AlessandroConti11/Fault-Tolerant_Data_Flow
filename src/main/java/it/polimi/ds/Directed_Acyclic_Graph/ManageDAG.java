package it.polimi.ds.Directed_Acyclic_Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.collections.functors.OnePredicate;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import com.google.protobuf.ByteString;

import it.polimi.ds.Exceptions;
import it.polimi.ds.WorkerManager;
import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;

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

    public static final int maxTasksPerTaskManger = WorkerManager.TASK_SLOTS;
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
     * List of operations to be calculated in a single operation group.
     * A group of operations consists of operations until a key change operation
     * occurs.
     */
    private ArrayList<List<Triplet<OperatorName, FunctionName, Integer>>> operationsGroup = new ArrayList<>();

    /**
     * Defines the group interval for putting a checkpoint
     */
    private int checkpointInterval;

    /**
     * This types is like a struct, it just needs to hold the data
     */
    private class Computation {
        static final long INVALID_GROUP = -1;

        final List<Data> init_data;

        final Vector<Long> fragments_received = new Vector<>();
        long current_checkpoint_group = checkpointInterval - 1;
        DataRequest.Builder current_checkpoint = DataRequest.newBuilder();
        long last_checkpoint_group = INVALID_GROUP;
        DataRequest.Builder last_checkpoint;

        Object checkpoint_lock = new Object();

        public Computation(List<Data> init_data) {
            this.init_data = init_data;
        }

        @Override
        public String toString() {
            return "Computation{fragments_received:" + fragments_received
                    + ", current_checkpoint_group:" + current_checkpoint_group
                    + ", last_checkpoint_group:" + last_checkpoint_group
                    + "}";
        }
    }

    private volatile long computation_count = 0;
    private ConcurrentMap<Long, Computation> running_computations = new ConcurrentHashMap<>();
    private Object new_computation_lock = new Object();

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

        this.maxTasksPerGroup = (getNumberOfTaskManager() * maxTasksPerTaskManger) / this.getNumberOfGroups();
        if (this.maxTasksPerGroup == 0) {
            throw new Exceptions.NotEnoughResourcesException();
        }

        // sets the effective number of tasks used
        this.setNumberOfTask(this.getNumberOfGroups() * this.maxTasksPerGroup);

        long taskManagerID = 0;
        taskIsInTaskManager.put(0L, taskManagerID);
        for (long i = 1L; i < (long) getNumberOfTask(); i++) {
            taskIsInTaskManager.put(i, (i + 1) % maxTasksPerTaskManger == 0 ? taskManagerID++ : taskManagerID);
        }

        // divide the task in group & assign the group order
        this.divideTaskInGroup();

        /// TODO: maybe take this as input or compute it with some heuristic?
        checkpointInterval = 2;
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
     * Waits for the first line of checkpoints to be free, then returns the data to
     * be sent
     */
    public long newComputation(List<Data> init_data) {
        synchronized (new_computation_lock) {
            while (true) {
                System.out.println(running_computations);
                var maybe_comp = running_computations.entrySet().stream()
                        .filter(comp -> comp.getValue().last_checkpoint_group <= checkpointInterval)
                        .findFirst();

                if (maybe_comp.isPresent()) {
                    try {
                        System.out.println("New computation lock");
                        new_computation_lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    break;
                }
            }

            synchronized (this) {
                running_computations.put(computation_count, new Computation(init_data));
            }
        }

        long ret = computation_count;
        computation_count += 1;
        return ret;
    }

    public Map<Long, Computation> getComputations() {
        return running_computations;
    }

    public synchronized void finishComputation(long computation_id) {
        System.out.println("Computation finished");

        releaseLocks(computation_id);
        running_computations.remove(computation_id);
    }

    public List<DataRequest.Builder> getDataRequestsForGroup(long computation_id, long group_id) {
        return generateDataRequests(running_computations.get(computation_id).init_data, computation_id, group_id);
    }

    private List<DataRequest.Builder> generateDataRequests(List<Data> data, long comp_id, long group_id) {
        List<DataRequest.Builder> ret = new ArrayList<>(maxTasksPerGroup);
        for (int i = 0; i < maxTasksPerGroup; i++) {
            ret.add(DataRequest.newBuilder());
        }
        for (var d : data) {
            var task_data = ret.get(d.getKey() % maxTasksPerGroup);
            task_data.addData(d);
        }

        return ret;
    }

    /**
     * @param task_id is one of the tasks in those groups
     * @return A list with all the groups of a stage of a compution, i.e. they are
     *         from the same checkpoint group
     */
    public List<Long> getStageFromTask(long task_id) {
        long grp = groupFromTask(task_id).get();
        List<Long> ret = new Vector<>();

        for (int i = 0; i < checkpointInterval; i++) {
            ret.add(grp - i);
        }

        return ret;
    }

    public int getMaxTasksPerGroup() {
        return maxTasksPerGroup;
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

    public boolean isCheckpoint(long groupId) {
        return groupId < getNumberOfGroups() - 1 && (groupId % checkpointInterval) == checkpointInterval - 1;
    }

    public List<DataRequest.Builder> getLastCheckpoint(long computation_id) {
        return generateDataRequests(
                running_computations.get(computation_id).last_checkpoint.getDataList(),
                computation_id,
                running_computations.get(computation_id).last_checkpoint_group);
    }

    public long getGroupOfLastCheckpoint(long computation_id) {
        return running_computations.get(computation_id).last_checkpoint_group;
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
     * Setter --> sets the followers of the groups.
     *
     * @param followerGroup the followers of the groups.
     */
    public void setFollowerGroup(HashMap<Long, Long> followerGroup) {
        this.followerGroup = followerGroup;
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

    }

    public Optional<Long> getNextFreeTaskManager() {
        if (freeTaskManagers.size() > 0) {
            return Optional.of(freeTaskManagers.remove(freeTaskManagers.size() - 1));
        }

        return Optional.empty();
    }

    public Optional<Long> groupFromTask(long taskId) {
        for (Long group : tasksInGroup.keySet()) {
            if (tasksInGroup.get(group).contains((Long) taskId)) {
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

    public Set<Long> getManagersOfGroup(long group_id) {
        if (!tasksInGroup.containsKey(group_id)) {
            /// This is the last group. So no task manager is needed.
            assert group_id == tasksInGroup.size() : "Group is not the last";

            return new HashSet<>();
        }

        var nextTasks = tasksInGroup.get(group_id)
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
    public List<Long> getGroupsOfTaskManager(long taskManagerId) {
        List<Long> result = new ArrayList<>();

        // Find all tasks in a task manager.
        List<Long> matchingTid = getTasksOfTaskManager(taskManagerId);

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

    public int getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * Saves the current state of data and operations at a specific checkpoint.
     *
     * @param checkpointRequest the request object containing the group ID, data,
     *                          and list of operations to be saved.
     * @return true when the checkpoint is completed, thus it holds all the data
     */
    public boolean saveCheckpoint(CheckpointRequest checkpointRequest) {
        if (checkpointRequest.getIsFromAnotherCheckpoint() != 0) {
            return true;
        }

        var comp = running_computations.get(checkpointRequest.getComputationId());
        assert comp != null : checkpointRequest.getComputationId() + " " + checkpointRequest.getSourceTaskId()
                + " should exist. Got " + running_computations;
        var grp = groupFromTask(checkpointRequest.getSourceTaskId()).get();

        assert comp.fragments_received.size() < maxTasksPerGroup : "Received request for a finished checkpoint";
        assert grp <= comp.current_checkpoint_group : "Got checkpoint from unexpected source expect: "
                + comp.current_checkpoint_group + " got: " + groupFromTask(checkpointRequest.getSourceTaskId()).get();

        /// Skips the last since it can't be a checkpoint by definition
        while (comp.current_checkpoint_group < getNumberOfGroups() && grp != comp.current_checkpoint_group) {
            System.out.println("Waiting for the messages from the last checkpoint to arrive curr: " + grp
                    + " waiting for " + comp.current_checkpoint_group);
            synchronized (comp.checkpoint_lock) {
                try {
                    comp.checkpoint_lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            grp = groupFromTask(checkpointRequest.getSourceTaskId()).get();
        }

        /// TODO: assert that this comes from a repeated computation
        if (comp.fragments_received.contains(checkpointRequest.getSourceTaskId()))
            return false;

        comp.current_checkpoint.addAllData(checkpointRequest.getDataList());
        comp.fragments_received.add(checkpointRequest.getSourceTaskId());

        if (comp.fragments_received.size() == maxTasksPerGroup) {
            return true;
        }

        return false;
    }

    public void moveForwardWithComputation(long comp_id) {
        var comp = running_computations.get(comp_id);

        comp.last_checkpoint = comp.current_checkpoint;
        comp.last_checkpoint_group = comp.current_checkpoint_group;
        comp.current_checkpoint_group += checkpointInterval;
        comp.fragments_received.clear();

        releaseLocks(comp_id);
    }

    public void releaseLocks(long comp_id) {
        System.out.println("Releasing locks for comp " + comp_id);

        /// TODO: Handle this in a better way, this is just a hacky fix to handle the
        /// last flushing becoming null before receiving the flushresponse
        var comp = running_computations.get(comp_id);
        if (comp != null) {
            synchronized (comp.checkpoint_lock) {
                comp.checkpoint_lock.notifyAll();
            }
        }

        synchronized (resume_computation_lock) {
            resume_computation_lock.notifyAll();
        }

        synchronized (new_computation_lock) {
            new_computation_lock.notifyAll();
        }
    }

    public Optional<Long> getManagerOfTask(long taskId) {
        if (taskIsInTaskManager.containsKey(taskId)) {
            return Optional.of(taskIsInTaskManager.get(taskId));
        }

        return Optional.empty();
    }

    public boolean isLastGroup(long group_id) {
        return this.followerGroup.get(group_id) == null;
    }

    public Optional<Long> getCurrentComputationOfGroup(long group_id) {
        var computations = running_computations.values().stream()
                .filter(comp -> comp.last_checkpoint_group <= group_id && comp.current_checkpoint_group >= group_id)
                .collect(Collectors.toList());
        assert computations.size() <= 1 : "Somehow there are 2 overlapping computations";

        if (computations.size() == 0)
            return Optional.empty();
        for (var entry : running_computations.entrySet()) {
            if (entry.getValue().equals(computations.get(0))) {
                return Optional.of(entry.getKey());
            }
        }

        assert false : "Uncreachable";
        return Optional.empty();
    }

    private Object resume_computation_lock = new Object();

    public void waitForNextStageToBeFree(long task_id) {
        long grp = groupFromTask(task_id).get();
        assert isCheckpoint(grp);

        long next_grp = grp + checkpointInterval;

        while (getCurrentComputationOfGroup(next_grp).isPresent()) {
            System.out.println("LOCK on computation " + getCurrentComputationOfGroup(next_grp).get());
            System.out.println(running_computations);
            synchronized (resume_computation_lock) {
                try {
                    resume_computation_lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
     * - Tid1
     * - check point
     * 
     * operation to execute
     * last data
     * a chi mando i dati
     */
}
