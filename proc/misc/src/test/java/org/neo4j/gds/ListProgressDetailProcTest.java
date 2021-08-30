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
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ListProgressDetailProcTest extends BaseProgressTest {

    String jobId;

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            ProgressTrackingTestProc.class
        );

        runQuery("CALL gds.test.track");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        AtomicReference<JobId> jobIdRef = new AtomicReference<>();
        runQueryWithRowConsumer(
            "CALL gds.beta.listProgress() YIELD jobId RETURN jobId",
            row -> jobIdRef.set(JobId.fromString(row.getString("jobId")))
        );
        this.jobId = jobIdRef.get().asString();
    }

    @Test
    void shouldReturnProgressDetail() {
        assertCypherResult(
            "CALL gds.beta.listProgress('" + jobId + "')" +
            "YIELD taskName, progressBar, progress, timeStarted, elapsedTime, status, jobId " +
            "RETURN taskName, progressBar, progress, timeStarted, elapsedTime, status, jobId ",
            List.of(
                Map.of(
                    "taskName", "|-- root",
                    "progressBar", "[####~~~~~~]",
                    "progress", "42.86%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "    |-- iterative",
                    "progressBar", "[#######~~~]",
                    "progress", "75%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[##########]",
                    "progress", "100%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.FINISHED.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[#####~~~~~]",
                    "progress", "50%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name(),
                    "jobId", jobId
                ),
                Map.of(
                    "taskName", "    |-- leaf",
                    "progressBar", "[~~~~~~~~~~]",
                    "progress", "0%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
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
            DurationValue elapsedTime = (DurationValue) row.get("elapsedTime");

            assertThat(timeStarted.toNanoOfDay()).isEqualTo(0L);
            assertThat(elapsedTime.compareTo(DurationValue.duration(0, 0, 0, 0))).isEqualTo(0);
        });
    }

    public static class ProgressTrackingTestProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

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

            var taskProgressTracker = new TaskProgressTracker(task, ProgressLogger.NULL_LOGGER, progress, taskRegistry);

            taskProgressTracker.beginSubTask(); // root
            taskProgressTracker.beginSubTask(); // iterative
            taskProgressTracker.beginSubTask(); // leafIterative 1
            taskProgressTracker.logProgress(2); // log 2/2
            taskProgressTracker.endSubTask();
            taskProgressTracker.beginSubTask(); // leafIterative 2
            taskProgressTracker.logProgress(1); // log 1/2

            return Stream.empty();
        }
    }
}
