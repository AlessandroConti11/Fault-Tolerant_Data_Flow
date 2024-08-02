package it.polimi.ds;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import it.polimi.ds.Directed_Acyclic_Graph.ManageDAG;
import it.polimi.ds.proto.CheckpointRequest;
import it.polimi.ds.proto.Data;

public class DAGTest {

    @Test
    public void empytProgram() {
        /// Empty program should just return the input values without any changes
        assertDoesNotThrow(() -> {
            new ManageDAG(ByteString.copyFromUtf8(""), 1);
        });
    }

    @Test
    public void noWorkerManagers() {
        /// No WorkerManager able to execute the program
        assertThrowsExactly(Exceptions.NotEnoughResourcesException.class, () -> {
            new ManageDAG(ByteString.copyFromUtf8("map;add;1"), 0);
        });
    }

    @Test
    public void tooManyTasks() {
        /// Not enough resources to execute the program
        assertThrowsExactly(Exceptions.NotEnoughResourcesException.class, () -> {
            String program = "change_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1";
            new ManageDAG(ByteString.copyFromUtf8(program), 1);
        });
    }

    @Test
    public void justEnoughTasks() {
        /// Not enough resources to execute the program
        assertDoesNotThrow(() -> {
            String program = "change_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\n";
            new ManageDAG(ByteString.copyFromUtf8(program), 1);
        });
    }

    @Test
    public void freeWorkerManagers() throws Exception {
        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8("map;add;1"), 5);
        assertDoesNotThrow(() -> {
            assertEquals(4L, dag.getNextFreeTaskManager().get());
            assertEquals(3L, dag.getNextFreeTaskManager().get());
            assertEquals(2L, dag.getNextFreeTaskManager().get());
            assertEquals(1L, dag.getNextFreeTaskManager().get());
            assertEquals(0L, dag.getNextFreeTaskManager().get());
        });
        assertThrowsExactly(NoSuchElementException.class, () -> {
            dag.getNextFreeTaskManager().get();
        });

        dag.addFreeTaskManager(3);
        assertDoesNotThrow(() -> {
            assertEquals(3L, dag.getNextFreeTaskManager().get());
        });

        /// Can't re-add a WorkerManager that doesn't exist
        assertThrowsExactly(NoSuchElementException.class, () -> {
            dag.addFreeTaskManager(15);
        });

    }

    @Test
    public void operationGroups() throws Exception {
        String program = "change_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1";

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4);

        var ops = dag.getOperationsGroup();
        assertEquals(8, ops.size());

        /// Reduce works in the same way as a change_key when grouping operations
        program = "change_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nchange_key;add;1"
                + "\nreduce;add;1";

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 4);

        ops = dag.getOperationsGroup();
        assertEquals(8, ops.size());
    }

    @Test
    public void numberOfTasks() throws Exception {
        String program = "change_key;add;1"
                + "\nchange_key;add;1";

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4);

        assertEquals(10, dag.getMaxTasksPerGroup());
        assertEquals(20, dag.getNumberOfTask());
        for (int i = 0; i < 3; i++) {
            assertEquals(true, ManageDAG.maxTasksPerTaskManger >= dag.getTaskInTaskManager(i).stream()
                    .count());
        }

        for (long i = 0; i < dag.getNumberOfTask(); i++) {
            assertEquals(i / ManageDAG.maxTasksPerTaskManger, dag.getTaskIsInTaskManager().get(i));
        }

        program = "change_key;add;1"
                + "\nchange_key;add;1";

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 3);
        assertEquals(7, dag.getMaxTasksPerGroup());
        assertEquals(14, dag.getNumberOfTask());
        for (int i = 0; i < 3; i++) {
            assertEquals(true, ManageDAG.maxTasksPerTaskManger >= dag.getTaskInTaskManager(i).stream()
                    .count());
        }

        for (long i = 0; i < dag.getNumberOfTask(); i++) {
            assertEquals(i / ManageDAG.maxTasksPerTaskManger, dag.getTaskIsInTaskManager().get(i));
        }
    }

    @Test
    public void groups() throws Exception {
        String program = "change_key;add;1"
                + "\nchange_key;add;1";

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4);
        assertEquals(0, dag.groupFromTask(0).get());
        assertEquals(1, dag.groupFromTask(10).get());
        assertEquals(Optional.empty(), dag.groupFromTask(20));

        program = "filter;not_equal;55\n" +
                "change_key;add;70\n" +
                "map;add;13\n" +
                "filter;lower_or_equal;93\n" +
                "change_key;add;75\n" +
                "filter;not_equal;11\n" +
                "map;mult;77\n" +
                "filter;not_equal;19\n" +
                "change_key;add;75";

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 2);
        assertEquals(0, dag.groupFromTask(0).get());
        assertEquals(1, dag.groupFromTask(3).get());
        assertEquals(2, dag.groupFromTask(6).get());
        assertEquals(Optional.empty(), dag.groupFromTask(10));
        assertEquals(0, dag.getGroupsOfTaskManager(0).get(0));
        assertEquals(1, dag.getGroupsOfTaskManager(0).get(1));
        assertEquals(1, dag.getGroupsOfTaskManager(1).get(0));
        assertEquals(2, dag.getGroupsOfTaskManager(1).get(1));

        assertEquals(true, dag.getManagersOfNextGroup(0).containsAll(List.of(0L, 1L)));
        assertEquals(true, dag.getManagersOfNextGroup(1).containsAll(List.of(1L)));
        /// 2 is the last group
        assertEquals(0, dag.getManagersOfNextGroup(2).size());
    }

    @Test
    public void computationCrash() throws Exception {
        String program = "filter;not_equal;55\n" +
                "change_key;add;70\n" +
                "map;add;13\n" +
                "filter;lower_or_equal;93\n" +
                "change_key;add;75\n" +
                "filter;not_equal;11\n" +
                "map;mult;77\n" +
                "filter;not_equal;19\n" +
                "change_key;add;75";

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 2);

        long comp_id = dag.newComputation(List.of(Data.newBuilder().setKey(0).setValue(0).build()));
        assertEquals(0, comp_id);

        { /// new scope to re-use namee
            long crashed_non_important = 2;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_non_important);
            assertEquals(List.of(), impacted_groups);
        }

        { /// new scope to re-use namee
            long crashed_id = 0;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(0L, 1L), impacted_groups);

            var comp_opts = impacted_groups.stream()
                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                    .collect(Collectors.toList());
            assertEquals(List.of(Optional.of(0L), Optional.of(0L)), comp_opts);

            var comp_list = comp_opts.stream()
                    .flatMap(Optional::stream)
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(List.of(0L), comp_list);
            var impacted_cid = comp_list.get(0);
            long grp = dag.getGroupOfLastCheckpoint(impacted_cid);
            assertEquals(-1L, grp);
        }

        var comp = CheckpointRequest.newBuilder()
            .addData(Data.newBuilder().setKey(0).setValue(0).build())
            .setComputationId(comp_id)
            .setGroupId(1L)
            .build();
        dag.saveCheckpoint(comp);
        dag.saveCheckpoint(comp);
        dag.saveCheckpoint(comp);
        assertThrowsExactly(AssertionError.class, () -> dag.saveCheckpoint(comp));

        { /// new scope to re-use namee
            long crashed_id = 0;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(0L, 1L), impacted_groups);

            var comp_opts = impacted_groups.stream()
                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                    .collect(Collectors.toList());
            assertEquals(List.of(Optional.empty(), Optional.empty()), comp_opts);

            var comp_list = comp_opts.stream()
                    .flatMap(Optional::stream)
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(List.of(), comp_list);
        }

        { /// new scope to re-use namee
            long crashed_id = 1;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(1L, 2L), impacted_groups);

            var comp_opts = impacted_groups.stream()
                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                    .collect(Collectors.toList());
            assertEquals(List.of(Optional.empty(), Optional.of(0L)), comp_opts);

            var comp_list = comp_opts.stream()
                    .flatMap(Optional::stream)
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(List.of(0L), comp_list);

            var impacted_cid = comp_list.get(0);
            long grp = dag.getGroupOfLastCheckpoint(impacted_cid);
            assertEquals(1L, grp);
        }
    }
}
