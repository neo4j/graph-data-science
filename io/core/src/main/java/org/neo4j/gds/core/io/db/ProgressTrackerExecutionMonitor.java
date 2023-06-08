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
package org.neo4j.gds.core.io.db;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ProgressTrackerExecutionMonitor implements CompatExecutionMonitor {

    private final Clock clock;
    private final long intervalMillis;

    private final ProgressTracker progressTracker;


    public static Task progressTask() {
        return Tasks.task(
            GraphStoreToDatabaseExporter.class.getSimpleName(),
            List.of()
        );
    }

    public static ExecutionMonitor of(
        ProgressTracker progressTracker,
        Clock clock,
        long time,
        TimeUnit unit
    ) {
        return Neo4jProxy.executionMonitor(new ProgressTrackerExecutionMonitor(progressTracker, clock, time, unit));
    }

    private ProgressTrackerExecutionMonitor(ProgressTracker progressTracker, Clock clock, long time, TimeUnit unit) {
        this.clock = clock;
        this.intervalMillis = unit.toMillis(time);
        this.progressTracker = progressTracker;
    }

    @Override
    public void initialize(DependencyResolver dependencyResolver) {
        this.progressTracker.beginSubTask();
    }

    @Override
    public void start(StageExecution execution) {
        progressTracker.logInfo(
            formatWithLocale(
                "%s :: Start",
                execution.getStageName()
            )
        );
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {
        progressTracker.logInfo(
            formatWithLocale(
                "%s :: Finished",
                execution.getStageName()
            )
        );
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        this.progressTracker.endSubTask();
        this.progressTracker.logInfo(additionalInformation);
    }

    @Override
    public void check(StageExecution execution) {

    }

    @Override
    public Clock clock() {
        return this.clock;
    }

    @Override
    public long checkIntervalMillis() {
        return this.intervalMillis;
    }
}
