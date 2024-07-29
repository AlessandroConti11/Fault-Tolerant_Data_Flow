package it.polimi.ds;

import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.Computation;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;

class Task {
    private final long id;
    private final long group_id;
    private final boolean is_checkpoint;
    private final int group_size;
    private final Computation computation;

    private volatile boolean has_all_data = false;
    private int data_count = 0;
    private List<Data> data;

    private List<Pair<Integer, Integer>>  result;

    public Task(long id, long group_id, Computation computation, boolean is_checkpoint, int group_size) {
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

    /**
     * Getter --> gets the group id.
     *
     * @return the id of the group to which the task belongs.
     */
    public long getGroup_id() {
        return group_id;
    }

    public boolean isReady() {
        return has_all_data;
    }

    public void reset() {
        has_all_data = false;
        data_count = 0;
        data.clear();
    }

    // TODO: Reset counted for new computation.
    public void addData(DataRequest req) {
        this.data.addAll(req.getDataList());
        if (req.getSourceRole() == Role.MANAGER) {
            data_count = group_size;
        } else {
            data_count++;
        }

        if (data_count == group_size) {
            has_all_data = true;

            synchronized (this) {
                this.notifyAll();
            }
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

        System.out.println(group_size);
        for (var d : result) {
            var task_data = ret.get(d.getValue0() % group_size);
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
    }

    public void waitForData() {
        synchronized (this) {
            try {
                this.wait();
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
}
