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

import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TestProgressTracker extends TaskProgressTracker {

    private final List<AtomicLong> progresses;

    public TestProgressTracker(
        Task baseTask,
        Log log,
        int concurrency,
        TaskRegistryFactory taskRegistryFactory
    ) {
        super(baseTask, log, concurrency, taskRegistryFactory);
        progresses = new ArrayList<>();
    }

    public List<AtomicLong> getProgresses() {
        return progresses;
    }

    @Override
    public void logProgress(long progress) {
        progresses.get(progresses.size() - 1).addAndGet(progress);
        super.logProgress(progress);
    }

    @Override
    public void beginSubTask() {
        super.beginSubTask();
        currentTask.ifPresent(__ -> progresses.add(new AtomicLong()));
    }

    @Override
    public void setVolume(long volume) {
        super.setVolume(volume);
        progresses.add(new AtomicLong());
    }
}
