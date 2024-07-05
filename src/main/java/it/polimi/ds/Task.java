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

import java.util.List;

class Task {
    private final long id;
    private final long group_id;
    private final boolean is_checkpoint;
    private final int group_size;
    private final Computation computation;

    private volatile boolean has_all_data = false;
    private int data_count = 0;
    private List<Data> data;

    public Task(long id, long group_id, Computation computation, boolean is_checkpoint, int group_size) {
        this.id = id;
        this.is_checkpoint = is_checkpoint;
        this.group_id = group_id;
        this.group_size = group_size;
        this.computation = computation;
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

    public List<Long> getSuccessorIds() {
        return computation.getManagerSuccessorIdsList();
    }

    /**
     * Perform the operation.
     *
     * @return the new data, after the operations have been performed.
     */
    public List<Pair<Integer, Integer>> execute() {
        // Data to compute.
        List<Pair<Integer, Integer>> dataToCompute = ManageCSVfile.readCSVinput(data);
        // Operation to be performed on data.
        List<Triplet<OperatorName, FunctionName, Integer>> operationToCompute = ManageCSVfile
                .readCSVoperation(computation);

        return new Operator().operations(operationToCompute, dataToCompute);
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
}
