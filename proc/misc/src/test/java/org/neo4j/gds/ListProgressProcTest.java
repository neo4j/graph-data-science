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
package org.neo4j.gds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.RenamesCurrentThread;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.embeddings.fastrp.StreamResult;
import org.neo4j.gds.extension.FakeClockExtension;
import org.neo4j.gds.extension.Inject;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.time.FakeClock;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@FakeClockExtension
class ListProgressProcTest extends BaseProgressTest {

    @Inject
    public FakeClock fakeClock;

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            GraphGenerateProc.class,
            BaseProgressTestProc.class,
            ProgressLoggingTestFastRP.class
        );
    }

    @Test
    void shouldNotFailWhenTheTaskStoreIsEmpty() {
        assertDoesNotThrow(() -> runQuery("CALL gds.beta.listProgress()"));
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        assertCypherResult(
            "CALL gds.beta.listProgress() " +
            "YIELD username, taskName, progress, progressBar, status, timeStarted, elapsedTime " +
            "RETURN username, taskName, progress, progressBar, status, timeStarted, elapsedTime ",
            List.of(
                Map.of(
                    "taskName","foo",
                    "username", Username.EMPTY_USERNAME.username(),
                    "progress", "33.33%",
                    "progressBar", "[###~~~~~~~]",
                    "status", "RUNNING",
                    "timeStarted", LocalTime.ofInstant(fakeClock.instant(), ZoneId.systemDefault()),
                    "elapsedTime", "0 seconds"
                )
            )
        );
    }

    @Test
    void shouldReturnValidJobId() {
        runQuery("CALL gds.test.pl('foo')");
        runQueryWithRowConsumer(
            "CALL gds.beta.listProgress() YIELD jobId RETURN jobId",
            row -> assertDoesNotThrow(() -> new JobId(row.getString("jobId")))
        );
    }

    @Test
    void listOnlyFirstProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        runQuery("CALL gds.test.pl('bar')");
        assertCypherResult(
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName ORDER BY taskName",
            List.of(
                Map.of("taskName", "bar"),
                Map.of("taskName","foo")
            )
        );
    }

    @Test
    void progressIsListedFilteredByUser() {
        runQuery("Alice", "CALL gds.test.pl('foo')");
        runQuery("Bob", "CALL gds.test.pl('bar')");

        var aliceResult = runQuery(
            "Alice",
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(aliceResult).containsExactlyInAnyOrder(Map.of("taskName", "foo"));

        var bobResult = runQuery(
            "Bob",
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(bobResult).containsExactlyInAnyOrder(Map.of("taskName", "bar"));
    }

    @Test
    void progressLoggerShouldEmitProgressEventsOnActualAlgoButClearProgressEventsOnLogFinish() {
        try (var ignored = RenamesCurrentThread.renameThread("Test worker")) {
            runQuery("CALL gds.beta.graph.generate('foo', 100, 5)");
            runQuery("CALL gds.test.fakerp('foo', {embeddingDimension: 42})");

            assertCypherResult(
                "CALL gds.beta.listProgress() YIELD taskName, progress RETURN taskName, progress",
                List.of(
                    Map.of("taskName", "FastRP", "progress", "100%")
                )
            );
        }
    }

    public static class ProgressLoggingTestFastRP extends FastRPStreamProc {

        @Override
        @Procedure("gds.test.fastrp")
        public Stream<StreamResult> stream(
            @Name(value = "graphName") String graphName,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return super.stream(graphName, configuration);
        }

        @Procedure("gds.test.fakerp")
        public Stream<StreamResult> fakeStream(
            @Name(value = "graphName") String graphName,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            var taskRegistry = taskRegistryFactory.newInstance(new JobId());
            this.taskRegistryFactory = jobId -> new NonReleasingTaskRegistry(taskRegistry);
            return super.stream(graphName, configuration);
        }

    }
}
