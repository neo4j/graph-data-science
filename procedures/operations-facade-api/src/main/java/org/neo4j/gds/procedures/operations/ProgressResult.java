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
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.progress.registration.StoredTask;
import org.neo4j.gds.progress.tasks.Task;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

public record ProgressResult(
    String username,
    String jobId,
    String taskName,
    String progress,
    String progressBar,
    String status,
    LocalTime timeStarted,
    String elapsedTime
) {

    public static ProgressResult fromTaskStoreEntry(StoredTask storedTask) {
        return new ProgressResult(
            storedTask.user().getUsername(),
            storedTask.task(),
            storedTask.jobId(),
            storedTask.task().description()
        );
    }

    public static ProgressResult fromArrowProcess(
        String username,
        String jobId,
        String taskName,
        double relativeProgress,
        String status,
        long startTimeMillis,
        long finishOrNowMillis
    ) {
        return new ProgressResult(
            username,
            jobId,
            taskName,
            StructuredOutputHelper.computeProgress(relativeProgress),
            StructuredOutputHelper.progressBar(relativeProgress, 10),
            status,
            startTime(startTimeMillis),
            prettyElapsedTime(startTimeMillis, finishOrNowMillis)
        );
    }

    static ProgressResult fromTaskWithDepth(String username, Task task, JobId jobId, int depth) {
        var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
        return new ProgressResult(username, task, jobId, treeViewTaskName);
    }

    public ProgressResult(String username, Task task, JobId jobId, String taskName) {
        this(
            username,
            jobId.asString(),
            taskName,
            StructuredOutputHelper.computeProgress(task.getProgress()),
            StructuredOutputHelper.progressBar(task.getProgress(), 10),
            task.status().name(),
            startTime(task),
            prettyElapsedTime(task)
        );
    }

    private static LocalTime startTime(Task task) {
        if (task.hasNotStarted()) {
            return null;
        }
        return startTime(task.startTime());
    }

    private static LocalTime startTime(long startTimeMillis) {
        if (startTimeMillis < 0) {
            return null;
        }
        var instant = Instant.ofEpochMilli(startTimeMillis);
        return LocalTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private static String prettyElapsedTime(Task task) {
        if (task.hasNotStarted()) {
            return "Not yet started";
        }
        var finishTime = task.finishTime();
        var finishTimeOrNow = finishTime != -1
            ? finishTime
            : ClockService.clock().millis();
        return prettyElapsedTime(task.startTime(), finishTimeOrNow);
    }

    private static String prettyElapsedTime(long startTimeMillis, long finishOrNowMillis) {
        if (startTimeMillis < 0) {
            return "Not yet started";
        }
        var elapsedTime = finishOrNowMillis - startTimeMillis;
        return DurationFormatUtils.formatDurationWords(elapsedTime, true, true);
    }
}
