package it.polimi.ds;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import it.polimi.ds.Directed_Acyclic_Graph.ManageDAG;

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

	/// TODO: Stuff left to test:
	/// - numberOfTasks parameter, seems strange how it's set
	/// - do we need methods to add and remove TaskManagers?
	/// - assignment of both tasks and TaskManagers into groups
	/// - complicated getters, i.e. like getGroupsOfTaskManagers(tm_id)
	/// - followers/successors
	/// - checkpoints

}
