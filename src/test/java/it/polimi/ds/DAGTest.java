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
            new ManageDAG(ByteString.copyFromUtf8(""), 1, 1);
        });
    }

    @Test
    public void noWorkerManagers() {
        /// No WorkerManager able to execute the program
        assertThrowsExactly(Exceptions.NotEnoughResourcesException.class, () -> {
            new ManageDAG(ByteString.copyFromUtf8("map;add;1"), 0, 1);
        });
    }

    @Test
    public void tooManyTasks() {
        /// Not enough resources to execute the program
        assertThrowsExactly(Exceptions.NotEnoughResourcesException.class, () -> {
            String program = "change_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1";
            new ManageDAG(ByteString.copyFromUtf8(program), 1, 1);
        });
    }

    @Test
    public void justEnoughTasks() {
        /// Not enough resources to execute the program
        assertDoesNotThrow(() -> {
            String program = "change_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\nchange_key;add;1\n";
            new ManageDAG(ByteString.copyFromUtf8(program), 1, 1);
        });
    }

    @Test
    public void freeWorkerManagers() throws Exception {
        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8("map;add;1"), 5, 1);
        assertDoesNotThrow(() -> {
            assertEquals(4L, dag.getNextFreeTaskManager(0).get());
            assertEquals(3L, dag.getNextFreeTaskManager(0).get());
            assertEquals(2L, dag.getNextFreeTaskManager(0).get());
            assertEquals(1L, dag.getNextFreeTaskManager(0).get());
            assertEquals(0L, dag.getNextFreeTaskManager(0).get());
        });
        assertThrowsExactly(NoSuchElementException.class, () -> {
            dag.getNextFreeTaskManager(0).get();
        });

        dag.addFreeTaskManager(3);
        assertDoesNotThrow(() -> {
            assertEquals(3L, dag.getNextFreeTaskManager(0).get());
        });

        /// Can't re-add a WorkerManager that doesn't exist
        assertThrowsExactly(NoSuchElementException.class, () -> {
            dag.addFreeTaskManager(15);
        });

        ManageDAG dag2 = new ManageDAG(ByteString.copyFromUtf8("map;add;1"), 5, 2);

        assertDoesNotThrow(() -> {
            assertEquals(4L, dag2.getNextFreeTaskManager(1).get());
            assertEquals(3L, dag2.getNextFreeTaskManager(1).get());
            assertEquals(2L, dag2.getNextFreeTaskManager(0).get());
            assertEquals(1L, dag2.getNextFreeTaskManager(0).get());
            assertEquals(0L, dag2.getNextFreeTaskManager(0).get());
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

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4, 1);

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

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 4, 1);

        ops = dag.getOperationsGroup();
        assertEquals(8, ops.size());
    }

    @Test
    public void numberOfTasks() throws Exception {
        String program = "change_key;add;1"
                + "\nchange_key;add;1";

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4, 1);

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

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 3, 1);
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

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 4, 1);
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

        dag = new ManageDAG(ByteString.copyFromUtf8(program), 2, 1);
        assertEquals(0, dag.groupFromTask(0).get());
        assertEquals(1, dag.groupFromTask(3).get());
        assertEquals(2, dag.groupFromTask(6).get());
        assertEquals(Optional.empty(), dag.groupFromTask(10));
        assertEquals(0, dag.getGroupsOfTaskManager(0).get(0));
        assertEquals(1, dag.getGroupsOfTaskManager(0).get(1));
        assertEquals(1, dag.getGroupsOfTaskManager(1).get(0));
        assertEquals(2, dag.getGroupsOfTaskManager(1).get(1));

        /// get next group
        assertEquals(true, dag.getManagersOfGroup(1).containsAll(List.of(0L, 1L)));
        assertEquals(true, dag.getManagersOfGroup(2).containsAll(List.of(1L)));
        /// 2 is the last group
        assertEquals(0, dag.getManagersOfGroup(3).size());
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

        ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 2, 1);

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

        var comps = dag.getTasksOfGroup(1L).stream().map(t -> CheckpointRequest.newBuilder()
                .addData(Data.newBuilder().setKey(0).setValue(0).build())
                .setComputationId(comp_id)
                .setSourceTaskId(t)
                .build()).collect(Collectors.toList());

        boolean checkpoint_complete = dag.saveCheckpoint(comps.get(0));
        assertEquals(false, checkpoint_complete);
        checkpoint_complete = dag.saveCheckpoint(comps.get(0));
        assertEquals(false, checkpoint_complete);
        checkpoint_complete = dag.saveCheckpoint(comps.get(0));
        assertEquals(false, checkpoint_complete);

        /// The same checkpoints don't get counted twice
        assertDoesNotThrow(() -> dag.saveCheckpoint(comps.get(0)));
        checkpoint_complete = dag.saveCheckpoint(comps.get(1));
        assertEquals(false, checkpoint_complete);

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

        { /// new scope to re-use namee
            long crashed_id = 1;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(1L, 2L), impacted_groups);

            var comp_opts = impacted_groups.stream()
                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                    .collect(Collectors.toList());
            assertEquals(List.of(Optional.of(0L), Optional.empty()), comp_opts);

            var comp_list = comp_opts.stream()
                    .flatMap(Optional::stream)
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(List.of(0L), comp_list);
            var impacted_cid = comp_list.get(0);
            long grp = dag.getGroupOfLastCheckpoint(impacted_cid);
            assertEquals(-1L, grp);
        }

        checkpoint_complete = dag.saveCheckpoint(comps.get(2));
        assertEquals(true, checkpoint_complete);
        dag.moveForwardWithComputation(0);

        { /// new scope to re-use name
            long crashed_id = 0;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(0L, 1L), impacted_groups);

            var comp_opts = impacted_groups.stream()
                    .map(g_id -> dag.getCurrentComputationOfGroup(g_id))
                    .collect(Collectors.toList());
            assertEquals(List.of(Optional.empty(), Optional.of(0L)), comp_opts);

            var comp_list = comp_opts.stream()
                    .flatMap(Optional::stream)
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(List.of(0L), comp_list);
        }

        { /// new scope to re-use namee
            long crashed_id = 1;
            var impacted_groups = dag.getGroupsOfTaskManager(crashed_id);
            assertEquals(List.of(1L, 2L), impacted_groups);

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
            assertEquals(1L, grp);
        }
    }

    @Test
    public void lastCantBeCheckpoint() throws Exception {
        final String program = "filter;not_equal;55\n" +
                "change_key;add;70\n" +
                "map;add;13\n" +
                "filter;lower_or_equal;93\n" +
                "change_key;add;75\n" +
                "change_key;add;75\n" +
                "change_key;add;75\n" +
                "change_key;add;75\n" +
                "change_key;add;75\n" +
                "change_key;add;75\n" +
                "filter;not_equal;11\n" +
                "map;mult;77\n" +
                "filter;not_equal;19\n" +
                "change_key;add;75";

        final ManageDAG dag = new ManageDAG(ByteString.copyFromUtf8(program), 2, 1);
        final int num_of_grps = dag.getNumberOfGroups();
        final int check_interval = dag.getCheckpointInterval();
        for (int i = 0; i < num_of_grps - 1; i++) {
            if (i % check_interval == check_interval - 1) {
                assertEquals(true, dag.isCheckpoint(i));
            } else {
                assertEquals(false, dag.isCheckpoint(i));
            }
        }

        assertEquals(false, dag.isCheckpoint(num_of_grps - 1));
    }
}
