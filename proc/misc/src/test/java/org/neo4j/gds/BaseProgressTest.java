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

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.core.utils.progress.ProgressEventExtension;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.logging.Level;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.OptionalLong;
import java.util.stream.Stream;

public class BaseProgressTest extends BaseTest {

    protected final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    protected static final OptionalLong MAX_MEMORY_USAGE = OptionalLong.of(10);

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

    public static class ProgressTestProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.pl")
        public Stream<Bar> foo(
            @Name(value = "taskName") String taskName,
            @Name(value = "withMemoryEstimation", defaultValue = "false") boolean withMemoryEstimation
        ) {
            var leaf = Tasks.leaf("leaf", 3);
            var baseTask = Tasks.task(taskName, leaf);
            if (withMemoryEstimation) {
                baseTask.setMaxMemoryUsage(MAX_MEMORY_USAGE);
            }
            baseTask.start();
            progress.addTaskProgressEvent(baseTask);
            leaf.start();
            leaf.logProgress(1);
            progress.addTaskProgressEvent(leaf);

            return Stream.empty();
        }
    }

    public static class Bar {
        public final String field;

        public Bar(String field) {this.field = field;}
    }
}
