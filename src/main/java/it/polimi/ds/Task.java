package it.polimi.ds;

import com.google.protobuf.ByteString;
import it.polimi.ds.CSV.ManageCSVfile;
import it.polimi.ds.function.FunctionName;
import it.polimi.ds.function.Operator;
import it.polimi.ds.function.OperatorName;
import it.polimi.ds.proto.Computation;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.List;

class Task {
    private long id;
    private long group_id;
    private boolean is_checkpoint;
    private volatile boolean has_all_data = false;
    private Object data;

    public Task(long id, long group_id, boolean is_checkpoint) {
        this.id = id;
        this.is_checkpoint = is_checkpoint;
        this.group_id = group_id;
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

    /**
     * Perform the operation.
     *
     * @param computations the operation to perform.
     * @param data the data on which to perform the required operations.
     * @return the new data, after the operations have been performed.
     */
    public List<Pair<Integer, Integer>> execute(Computation computations, ByteString data) {
        //Data to compute.
        List<Pair<Integer, Integer>> dataToCompute = ManageCSVfile.readCSVinput(data);
        //Operation to be performed on data.
        List<Triplet<OperatorName, FunctionName, Integer>> operationToCompute = ManageCSVfile.readCSVoperation(computations);

        return new Operator().operations(operationToCompute, dataToCompute);
    }
}