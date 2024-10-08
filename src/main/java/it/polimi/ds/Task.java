package it.polimi.ds;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.ProtoComputation;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.Role;

import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

class Task {
    private long current_computation_id = -1;
    private final long id;
    private final long group_id;
    private final boolean is_checkpoint;
    private final int group_size;
    private final ProtoComputation computation;

    private final Object computation_lock = new Object();

    private ConcurrentMap<Long, List<Long>> received_data_from = new ConcurrentHashMap<>();
    private volatile boolean has_all_data = false;
    private int data_count = 0;
    private List<Data> data;
    private volatile boolean already_computed = false;
    private volatile boolean dont_send_back_checkpoint = false;

    private List<Pair<Integer, Integer>> result;

    public Task(long id, long group_id, ProtoComputation computation, boolean is_checkpoint, int group_size) {
        assert group_size > 0;
        assert id >= 0;

        this.id = id;
        this.is_checkpoint = is_checkpoint;
        this.group_id = group_id;
        this.group_size = group_size;
        this.computation = computation;
        this.data = new Vector<>();
    }

    /**
     * Getter --> gets the task id.
     *
     * @return the task id.
     */
    public long getId() {
        return id;
    }

    public long getComputationId() {
        return current_computation_id;
    }

    public boolean isReady() {
        return has_all_data;
    }

    public long getGroupId() {
        return group_id;
    }

    public synchronized void flushComputation(long comp_id) {
        received_data_from.remove(comp_id);
        has_all_data = false;
        data.clear();
        already_computed = false;
        data_count = 0;
    }

    public synchronized void addData(DataRequest req) {
        // System.out.println("Received data " + req.getTaskId() + " count " +
        // data_count + " " + group_size);

        dont_send_back_checkpoint = false;
        if (!received_data_from.containsKey(req.getComputationId())) {
            received_data_from.put(req.getComputationId(), new Vector<Long>());
        }

        if (received_data_from.get(req.getComputationId()).contains(req.getSourceTask())) {
            if (data_count == group_size) {
                synchronized (computation_lock) {
                    computation_lock.notifyAll();
                }
            }
            return;
        }

        assert !has_all_data;
        assert data_count < group_size;

        if (data_count == 0)
            current_computation_id = req.getComputationId();
        assert current_computation_id == req.getComputationId() : "current: " + current_computation_id + " received: "
                + req.getComputationId() + " task " + getId() + " data " + data_count;

        received_data_from.get(req.getComputationId()).add(req.getSourceTask());
        this.data.addAll(req.getDataList());
        if (req.getSourceRole() == Role.MANAGER) {
            data_count = group_size;
        } else {
            data_count++;
        }

        // System.out.println("Processed data " + req.getTaskId() + " count " +
        // data_count + " " + group_size);
        if (data_count == group_size) {
            has_all_data = true;

            synchronized (computation_lock) {
                computation_lock.notifyAll();
            }
        }
    }

    public List<Pair<Integer, Integer>> checkpointToResult(List<Data> datas) {
        List<Pair<Integer, Integer>> res = new ArrayList<>();
        for (Data d : datas) {
            res.add(Pair.with(d.getKey(), d.getValue()));
        }

        return res;
    }

    public boolean hasAlreadyComputed() {
        return already_computed;
    }

    public synchronized void restartFromCheckpoint(DataRequest req) {
        dont_send_back_checkpoint = false;

        if (req.getSourceRole() == Role.MANAGER || !hasAlreadyComputed()) {
            // assert data_count == 0 && data.isEmpty()
            // : "Received checkpoint while in the middle of a running computation " + req +
            // " ---- " + this;

            already_computed = true;
            dont_send_back_checkpoint = true;
            current_computation_id = req.getComputationId();
            this.result = checkpointToResult(req.getDataList());
            data_count = group_size;
        }

        if (!received_data_from.containsKey(req.getComputationId())) {
            /// TODO: It would be better to add all tasks to this
            received_data_from.put(req.getComputationId(), List.of(req.getSourceTask()));
        } else {
            if (received_data_from.get(req.getComputationId()).contains(req.getSourceTask())) {
                if (data_count == group_size) {
                    synchronized (computation_lock) {
                        computation_lock.notifyAll();
                    }
                }
                return;
            }
        }

        assert current_computation_id == req.getComputationId()
                : "Computation mismatch got " + req.getComputationId() + " expect " + current_computation_id;

        synchronized (computation_lock) {
            computation_lock.notifyAll();
        }
    }

    public Map<Long, List<Long>> getSuccessorMap() {
        return computation
                .getManagersMappingList().stream()
                /// No duplicate by design so it's fine
                .collect(Collectors.toMap(id -> id.getManagerSuccessorId(), id -> id.getTaskIdList(), (v1, v2) -> v1));
    }

    public List<DataRequest.Builder> getSuccessorsDataRequests() {
        List<DataRequest.Builder> ret = new ArrayList<>(group_size);
        for (int i = 0; i < group_size; i++) {
            ret.add(DataRequest.newBuilder());
        }

        for (var d : result) {
            var task_data = ret.get(Math.abs(d.getValue0()) % group_size);
            task_data.addData(Data.newBuilder()
                    .setKey(d.getValue0())
                    .setValue(d.getValue1()));
        }

        return ret;
    }

    /**
     * Perform the operation.
     *
     * @return the new data, after the operations have been performed.
     */
    public void execute() {
        // Data to compute.
        List<Pair<Integer, Integer>> dataToCompute = ManageCSVfile.readCSVinput(data);
        // Operation to be performed on data.groue
        List<Triplet<OperatorName, FunctionName, Integer>> operationToCompute = ManageCSVfile
                .readCSVoperation(computation);

        result = new Operator().operations(operationToCompute, dataToCompute);

        already_computed = true;
    }

    public void waitForData() {
        synchronized (computation_lock) {
            try {
                computation_lock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<Pair<Integer, Integer>> getResult() {
        return result;
    }

    public int getGroupSize() {
        return group_size;
    }

    public boolean isCheckpoint() {
        return is_checkpoint;
    }

    public boolean skipCheckpointWriteBack() {
        return dont_send_back_checkpoint;
    }
}
