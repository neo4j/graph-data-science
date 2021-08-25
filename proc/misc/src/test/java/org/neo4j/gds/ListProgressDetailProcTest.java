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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.ProgressEventExtension;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.logging.Level;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.values.storable.DurationValue;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ListProgressDetailProcTest extends BaseTest {

    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(GraphDatabaseSettings.store_internal_log_level, Level.DEBUG);
        builder.setConfig(ProgressFeatureSettings.progress_tracking_enabled, true);
        // make sure that we 1) have our extension under test and 2) have it only once
        builder.removeExtensions(ex -> ex instanceof ProgressEventExtension);
        builder.addExtension(new ProgressEventExtension(scheduler));
    }

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            ProgressTrackingTestProc.class
        );
    }

    @Test
    void shouldReturnProgressDetail() {
        runQuery("CALL gds.test.track");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        AtomicReference<JobId> jobIdRef = new AtomicReference<>();
        runQueryWithRowConsumer(
            "CALL gds.beta.listProgress() YIELD id RETURN id",
            row -> jobIdRef.set(JobId.fromString(row.getString("id")))
        );
        assertCypherResult(
            "CALL gds.beta.listProgressDetail('" + jobIdRef.get().asString() + "')" +
            "YIELD taskName, progressBar, progress, timeStarted, elapsedTime, status " +
            "RETURN taskName, progressBar, progress, timeStarted, elapsedTime, status ",
            List.of(
                Map.of(
                    "taskName", "|-- root",
                    "progressBar", "[####~~~~~~]",
                    "progress", "42.86%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name()
                ),
                Map.of(
                    "taskName", "    |-- iterative",
                    "progressBar", "[#######~~~]",
                    "progress", "75%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name()
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[##########]",
                    "progress", "100%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.FINISHED.name()
                ),
                Map.of(
                    "taskName", "        |-- leafIterative",
                    "progressBar", "[#####~~~~~]",
                    "progress", "50%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.RUNNING.name()
                ),
                Map.of(
                    "taskName", "    |-- leaf",
                    "progressBar", "[~~~~~~~~~~]",
                    "progress", "0%",
                    "timeStarted", instanceOf(LocalTime.class),
                    "elapsedTime", instanceOf(DurationValue.class),
                    "status", Status.PENDING.name()
                )
            )
        );
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

            var taskProgressTracker = new TaskProgressTracker(task, ProgressLogger.NULL_LOGGER, progress);

            taskProgressTracker.beginSubTask(); // root
            taskProgressTracker.beginSubTask(); // iterative
            taskProgressTracker.beginSubTask(); // leafIterative 1
            taskProgressTracker.logProgress(2); // log 2/2
            taskProgressTracker.endSubTask();
            taskProgressTracker.beginSubTask(); // leafIterative 2
            taskProgressTracker.logProgress(1); // log 1/2

            progress.addTaskProgressEvent(task);
            return Stream.empty();
        }
    }
}
