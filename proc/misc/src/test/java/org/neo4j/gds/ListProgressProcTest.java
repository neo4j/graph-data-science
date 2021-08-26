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
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.RenamesCurrentThread;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPFactory;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ListProgressProcTest extends BaseProgressTest {

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            GraphGenerateProc.class,
            ProgressTestProc.class,
            ProgressLoggingTestFastRP.class
        );
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        assertCypherResult(
            "CALL gds.beta.listProgress() " +
            "YIELD taskName, progress, progressBar, status, timeStarted, elapsedTime " +
            "RETURN taskName, progress, progressBar, status, timeStarted, elapsedTime ",
            List.of(
                Map.of(
                    "taskName","foo",
                    "stage", "0 of 2",
                    "progress", "33.33%",
                    "progressBar", "[###~~~~~~~]",
                    "status", "RUNNING",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class)
                )
            )
        );
    }

    @Test
    void shouldReturnValidJobId() {
        runQuery("CALL gds.test.pl('foo')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        runQueryWithRowConsumer(
            "CALL gds.beta.listProgress() YIELD jobId RETURN jobId",
            Map.of(),
            row -> assertDoesNotThrow(() -> JobId.fromString(row.getString("jobId")))
        );
    }

    @Test
    void listOnlyFirstProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        runQuery("CALL gds.test.pl('bar')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
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
        scheduler.forward(100, TimeUnit.MILLISECONDS);

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
            scheduler.forward(100, TimeUnit.MILLISECONDS);

            assertCypherResult(
                "CALL gds.beta.listProgress() YIELD taskName, progress RETURN taskName, progress",
                List.of(
                    Map.of("taskName", "FastRP", "progress", "100%")
                )
            );
        }
    }

    public static class ProgressLoggingTestProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.pl")
        public Stream<Bar> foo(
            @Name(value = "taskName") String taskName
        ) {
            var task = Tasks.task(taskName, Tasks.leaf("leaf", 3));
            var taskProgressTracker = new TaskProgressTracker(task, ProgressLogger.NULL_LOGGER, progress);
            taskProgressTracker.beginSubTask();
            taskProgressTracker.beginSubTask();
            taskProgressTracker.logProgress(1);
            return Stream.empty();
        }
    }

    public static class ProgressLoggingTestFastRP extends FastRPStreamProc {

        @Override
        @Procedure("gds.test.fastrp")
        public Stream<FastRPStreamProc.StreamResult> stream(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return super.stream(graphNameOrConfig, configuration);
        }

        @Procedure("gds.test.fakerp")
        public Stream<FastRPStreamProc.StreamResult> fakeStream(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            var tracker = this.progressEventTracker;
            this.progressEventTracker = new ProgressEventTracker() {
                @Override
                public void addTaskProgressEvent(Task task) {
                    tracker.addTaskProgressEvent(task);
                }

                @Override
                public void release() {
                    // skip the release because we want to observe the messages after the algo is done
                }
            };

            try {
                return super.stream(graphNameOrConfig, configuration);
            } finally {
                this.progressEventTracker = tracker;
            }
        }

        @Override
        protected AlgorithmFactory<FastRP, FastRPStreamConfig> algorithmFactory() {
            return new FastRPFactory<>();
        }
    }
}
