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
package org.neo4j.gds.core.utils.progress;

import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;

import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;

class TaskStoreCleaner implements TaskStoreListener {
    private final Duration retentionPeriod;
    private final ScheduledThreadPoolExecutor cleanerPool;
    private final TaskStore taskStore;


    public TaskStoreCleaner(TaskStore taskStore, Duration retentionPeriod) {
        this.taskStore = taskStore;
        this.retentionPeriod = retentionPeriod;
        this.cleanerPool = new ScheduledThreadPoolExecutor(1, ExecutorServiceUtil.DEFAULT_THREAD_FACTORY);
    }

    @Override
    public void onTaskAdded(UserTask userTask) {

    }

    @Override
    public void onTaskCompleted(UserTask userTask) {
        // avoid scheduler if task should be cleaned up immediately
        if (retentionPeriod.toMillis() == 0) {
            taskStore.remove(userTask.username(), userTask.jobId());
            return;
        }

        this.cleanerPool.schedule(
            () -> taskStore.remove(userTask.username(), userTask.jobId()),
            retentionPeriod.toMillis(),
            java.util.concurrent.TimeUnit.MILLISECONDS
        );
    }
}
