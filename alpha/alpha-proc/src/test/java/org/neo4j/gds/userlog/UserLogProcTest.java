/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.userlog;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryExtension;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.GlobalUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.wcc.WccStreamProc;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class UserLogProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        " CREATE (a : node {name: 'a'})\n" +
        " CREATE (b : node {name: 'b'})\n" +
        " CREATE (c : node {name: 'c'})\n" +

        "CREATE" +
        "(a)-[:EDGE1 {w: 3.0 , foo: 4.0 }]->(b),\n" +
        "(a)-[:EDGE2 {w: 2.0, foo: 4.0}]->(c)";

    @Inject
    IdFunction idFunction;


    private static final String GRAPH_NAME = "graph";
    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(ProgressFeatureSettings.progress_tracking_enabled, true);
        // make sure that we 1) have our extension under test and 2) have it only once
        builder.removeExtensions(ex -> ex instanceof TaskRegistryExtension);
        builder.addExtension(new TaskRegistryExtension());


    }


    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(FakeTaskProc.class, UserLogProc.class, WccStreamProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("foo")
            .withRelationshipProperty("w")
            .loadEverything()
            .yields();
        runQuery(createQuery);
    }

    private Map<String, Object> getMapOfTaskNameAndMessage(String taskName, String message) {
        return Map.of("taskName", taskName,
            "message", message
        );
    }

    @Test
    void shouldNotFailWhenThereAreNoWarnings() {
        assertDoesNotThrow(() -> runQuery("CALL gds.alpha.userLog()"));
    }

    @Test
    void userLogOutputMessages() {

        runQuery("CALL gds.test.fakewarnproc('foo')");

        assertCypherResult(
            "CALL gds.alpha.userLog() " +
            "YIELD taskName, message RETURN taskName, message ",
            List.of(
                getMapOfTaskNameAndMessage("foo", "This is a test warning"),
                getMapOfTaskNameAndMessage("foo", "This is another test warning")
            )
        );
    }

    @Test
    void userLogOutputMessageOnMultipleTasks() {

        runQuery("CALL gds.test.fakewarnproc('foo')");
        runQuery("CALL gds.test.fakewarnproc('foo2')");

        assertCypherResult(
            "CALL gds.alpha.userLog() " +
            "YIELD taskName, message RETURN taskName, message ORDER BY taskName ",
            List.of(
                getMapOfTaskNameAndMessage("foo", "This is a test warning"),
                getMapOfTaskNameAndMessage("foo", "This is another test warning"),
                getMapOfTaskNameAndMessage("foo2", "This is a test warning"),
                getMapOfTaskNameAndMessage("foo2", "This is another test warning")

            )

        );
    }

    @Test
    void userLogWorksWithWCCmodified() {
        var createQuery = GdsCypher.call(GRAPH_NAME)
            .algo("gds.wcc")
            .streamMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields();
        runQuery(createQuery);

        var createQuery2 = GdsCypher.call(GRAPH_NAME)
            .algo("gds.wcc")
            .streamMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields();
        runQuery(createQuery2);


        runQuery("CALL gds.test.fakewarnproc('foo')");

        assertCypherResult(
            "CALL gds.alpha.userLog() " +
            "YIELD taskName, message  RETURN taskName,message ORDER BY taskName",
            List.of(
                getMapOfTaskNameAndMessage(
                    "WCC",
                    "Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set."
                ),
                getMapOfTaskNameAndMessage(
                    "WCC",
                    "Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set."
                ),
                getMapOfTaskNameAndMessage("foo", "This is a test warning"),
                getMapOfTaskNameAndMessage("foo", "This is another test warning")
            )

        );
    }

    @Test
    void userLogOutputOnlyMostRecentTasks() {
        int numMostRecent = GlobalUserLogStore.MOST_RECENT;
        for (int i = 0; i < 2 * numMostRecent; ++i) {
            String currentFooId = "foo" + i;
            runQuery("CALL gds.test.fakewarnproc('" + currentFooId + "')");
        }
        var expectedQueryResult = IntStream.range(numMostRecent, 2 * numMostRecent).mapToObj(
            i -> Stream.of(
                getMapOfTaskNameAndMessage("foo" + i, "This is a test warning"),
                getMapOfTaskNameAndMessage("foo" + i, "This is another test warning")
            )
        ).flatMap(Function.identity()).collect(Collectors.toList());
        assertCypherResult(
            "CALL gds.alpha.userLog() " +
            "YIELD taskName, message RETURN taskName, message ORDER BY taskName",
            expectedQueryResult
        );
    }

    public static class FakeTaskProc {

        @Context
        public TaskRegistryFactory taskRegistryFactory;
        @Context
        public UserLogRegistryFactory userLogRegistryFactory;

        @Procedure("gds.test.fakewarnproc")
        public Stream<FakeResult> foo(
            @Name(value = "taskName") String taskName,
            @Name(value = "withMemoryEstimation", defaultValue = "false") boolean withMemoryEstimation,
            @Name(value = "withConcurrency", defaultValue = "false") boolean withConcurrency
        ) throws InterruptedException {
            var task = Tasks.task(taskName, Tasks.leaf("leaf", 3));

            var taskProgressTracker = new TaskProgressTracker(task, Neo4jProxy.testLog(), 1, new JobId(), taskRegistryFactory,
                userLogRegistryFactory
            );
            taskProgressTracker.beginSubTask();
            taskProgressTracker.beginSubTask();
            taskProgressTracker.logProgress(1);
            taskProgressTracker.logWarning("This is a test warning");
            taskProgressTracker.logWarning("This is another test warning");

            return Stream.empty();
        }
    }

    public static class FakeResult {
        public final String fakeField;

        public FakeResult(String fakeField) {this.fakeField = fakeField;}
    }
}
