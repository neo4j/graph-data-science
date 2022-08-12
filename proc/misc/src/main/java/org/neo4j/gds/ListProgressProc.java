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

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.DepthAwareTaskVisitor;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskTraversal;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.LocalTimeValue;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ListProgressProc extends BaseProc {

    static final int PROGRESS_BAR_LENGTH = 10;

    @Context
    public TaskStore taskStore;

    @Procedure("gds.beta.listProgress")
    @Description("List progress events for currently running tasks.")
    public Stream<ProgressResult> listProgress(
        @Name(value = "jobId", defaultValue = "") String jobId
    ) {
        return jobId.isBlank()
            ? jobsSummaryView()
            : jobDetailView(jobId);
    }

    private Stream<ProgressResult> jobsSummaryView() {
        return taskStore.query(username()).entrySet().stream().map(ProgressResult::fromTaskStoreEntry);
    }

    private Stream<ProgressResult> jobDetailView(String jobIdAsString) {
        var jobId = JobId.fromString(jobIdAsString);
        var task = taskStore.query(username(), jobId).orElseThrow(
            () -> new IllegalArgumentException(formatWithLocale("No task with job id `%s` was found.", jobIdAsString))
        );
        var jobProgressVisitor = new JobProgressVisitor(jobId);
        TaskTraversal.visitPreOrderWithDepth(task, jobProgressVisitor);
        return jobProgressVisitor.progressRowsStream();
    }

    public static class ProgressResult {
        public String jobId;
        public String taskName;
        public String progress;
        public String progressBar;
        public String status;
        public LocalTimeValue timeStarted;
        public String elapsedTime;

        static ProgressResult fromTaskStoreEntry(Map.Entry<JobId, Task> taskStoreEntry) {
            var jobId = taskStoreEntry.getKey();
            var task = taskStoreEntry.getValue();
            return new ProgressResult(task, jobId, task.description());
        }

        static ProgressResult fromTaskWithDepth(Task task, JobId jobId, int depth) {
            var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
            return new ProgressResult(task, jobId, treeViewTaskName);
        }

        public ProgressResult(Task task, JobId jobId, String taskName) {
            var progressContainer = task.getProgress();

            this.jobId = jobId.asString();
            this.taskName = taskName;
            this.progress = StructuredOutputHelper.computeProgress(progressContainer);
            this.progressBar = StructuredOutputHelper.progressBar(progressContainer, PROGRESS_BAR_LENGTH);
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

    public static class JobProgressVisitor extends DepthAwareTaskVisitor {

        private final JobId jobId;
        private final List<ProgressResult> progressRows;

        JobProgressVisitor(JobId jobId) {
            this.jobId = jobId;
            this.progressRows = new ArrayList<>();
        }

        Stream<ProgressResult> progressRowsStream() {
            return this.progressRows.stream();
        }

        @Override
        public void visit(Task task) {
            progressRows.add(ProgressResult.fromTaskWithDepth(task, jobId, depth()));
        }
    }
}
