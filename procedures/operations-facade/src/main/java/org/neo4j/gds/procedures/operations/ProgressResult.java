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
package org.neo4j.gds.procedures.operations;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.values.storable.LocalTimeValue;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

public class ProgressResult {
    public String username;
    public String jobId;
    public String taskName;
    public String progress;
    public String progressBar;
    public String status;
    public LocalTimeValue timeStarted;
    public String elapsedTime;

    static ProgressResult fromTaskStoreEntry(TaskStore.UserTask userTask) {
        return new ProgressResult(
            userTask.username(),
            userTask.task(),
            userTask.jobId(),
            userTask.task().description()
        );
    }

    static ProgressResult fromTaskWithDepth(String username, Task task, JobId jobId, int depth) {
        var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
        return new ProgressResult(username, task, jobId, treeViewTaskName);
    }

    public ProgressResult(String username, Task task, JobId jobId, String taskName) {
        var progressContainer = task.getProgress();

        this.jobId = jobId.asString();
        this.taskName = taskName;
        this.username = username;
        this.progress = StructuredOutputHelper.computeProgress(progressContainer);
        this.progressBar = StructuredOutputHelper.progressBar(progressContainer, 10);
        this.status = task.status().name();
        this.timeStarted = localTimeValue(task);
        this.elapsedTime = prettyElapsedTime(task);
    }

    private LocalTimeValue localTimeValue(Task task) {
        if (task.hasNotStarted()) {
            return null;
        }
        return LocalTimeValue.localTime(LocalTime.ofInstant(
            Instant.ofEpochMilli(task.startTime()),
            ZoneId.systemDefault()
        ));
    }

    private String prettyElapsedTime(Task task) {
        if (task.hasNotStarted()) {
            return "Not yet started";
        }
        var finishTime = task.finishTime();
        var finishTimeOrNow = finishTime != -1
            ? finishTime
            : ClockService.clock().millis();
        var elapsedTime = finishTimeOrNow - task.startTime();
        return DurationFormatUtils.formatDurationWords(elapsedTime, true, true);
    }
}
