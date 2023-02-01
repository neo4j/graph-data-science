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
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.FakeClockExtension;
import org.neo4j.procedure.Procedure;
import org.neo4j.time.FakeClock;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.nullValue;

@FakeClockExtension
class ListProgressDetailProcTest extends BaseProgressTest {

    private String jobId;

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            ProgressTrackingTestProc.class
        );

        runQuery("CALL gds.test.track");
        AtomicReference<JobId> jobIdRef = new AtomicReference<>();
        runQueryWithRowConsumer(
            "CALL gds.beta.listProgress() YIELD jobId RETURN jobId",
            row -> jobIdRef.set(new JobId(row.getString("jobId")))
        );
        this.jobId = jobIdRef.get().asString();
    }

    @Test
    void shouldReturnProgressDetail() {
        var expectedLocalTime = LocalTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
        assertCypherResult(
            "CALL gds.beta.listProgress('" + jobId + "')" +
            "YIELD taskName, progressBar, progress, timeStarted, elapsedTime, status, jobId " +
            "RETURN taskName, progressBar, progress, timeStarted, elapsedTime, status, jobId ",
            List.of(
                Map.of(
                    "taskName", "|-- root",
                    "progressBar", "[####~~~~~~]",
                    "progress", "42.86%",
                    "timeStarted", expectedLocalTime,
                    "elapsedTime", "42 seconds",
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "    |-- iterative",
                    "progressBar", "[#######~~~]",
                    "progress", "75%",
                    "timeStarted", expectedLocalTime,
                    "elapsedTime", "42 seconds",
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[##########]",
                    "progress", "100%",
                    "timeStarted", expectedLocalTime,
                    "elapsedTime", "42 seconds",
                    "status", Status.FINISHED.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[#####~~~~~]",
                    "progress", "50%",
                    "timeStarted", expectedLocalTime.plusSeconds(42),
                    "elapsedTime", "0 seconds",
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "    |-- leaf",
                    "progressBar", "[~~~~~~~~~~]",
                    "progress", "0%",
                    "timeStarted", nullValue(),
                    "elapsedTime", "Not yet started",
                    "status", Status.PENDING.name(),
                    "jobId", jobId
                )
            )
        );
    }

    @Test
    void shouldReturnZeroTimesWhenTaskHasNotStarted() {
        var query = "CALL gds.beta.listProgress('" + jobId + "') " +
                    "YIELD status, timeStarted, elapsedTime " +
                    "WHERE status = 'PENDING' " +
                    "RETURN timeStarted, elapsedTime";

        runQueryWithRowConsumer(query, row -> {
            LocalTime timeStarted = (LocalTime) row.get("timeStarted");
            var elapsedTime = row.getString("elapsedTime");

            assertThat(timeStarted).isNull();
            assertThat(elapsedTime).isEqualTo("Not yet started");
        });
    }

    public static class ProgressTrackingTestProc extends BaseProc {

        @Procedure("gds.test.track")
        public Stream<ListProgressProcTest.Bar> foo() {
            var task = Tasks.task(
                "root",
                Tasks.iterativeFixed(
                    "iterative",
                    () -> List.of(Tasks.leaf("leafIterative", 2)),
                    2
                ),
                Tasks.leaf("leaf", 3)
            );

            var taskProgressTracker = new TaskProgressTracker(task, Neo4jProxy.testLog(), 1, executionContext().taskRegistryFactory());

            taskProgressTracker.beginSubTask(); // root
            taskProgressTracker.beginSubTask(); // iterative
            taskProgressTracker.beginSubTask(); // leafIterative 1
            taskProgressTracker.logProgress(2); // log 2/2
            ((FakeClock) ClockService.clock()).forward(42, TimeUnit.SECONDS);
            taskProgressTracker.endSubTask();
            taskProgressTracker.beginSubTask(); // leafIterative 2
            taskProgressTracker.logProgress(1); // log 1/2

            return Stream.empty();
        }
    }
}
