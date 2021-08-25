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

import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.ProgressEvent;
import org.neo4j.gds.core.utils.progress.ProgressEventStore;
import org.neo4j.gds.core.utils.progress.tasks.DepthAwareTaskVisitor;
import org.neo4j.gds.core.utils.progress.tasks.IterativeTask;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskTraversal;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalTimeValue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ListProgressProc extends BaseProc {

    static final int PROGRESS_BAR_LENGTH = 10;

    @Context
    public ProgressEventStore progress;

    @Procedure("gds.beta.listProgress")
    @Description("List progress events for currently running tasks.")
    public Stream<ProgressResult> listProgress() {
        return progress.query(username()).stream().map(ProgressResult::fromProgressEvent);
    }

    @Procedure("gds.beta.listProgressDetail")
    @Description("List detailed progress events for the specified job id.")
    public Stream<ProgressResult> listProgressDetail(
        @Name(value = "jobId") String jobId
    ) {
        var progressEvent = progress.query(username(), JobId.fromString(jobId));
        var task = progressEvent.task();
        var jobProgressVisitor = new JobProgressVisitor(progressEvent.jobId());
        TaskTraversal.visitPreOrderWithDepth(task, jobProgressVisitor);
        return jobProgressVisitor.progressRows().stream();
    }

    public static class ProgressResult {
        public String jobId;
        public String taskName;
        public String progress;
        public String progressBar;
        public String status;
        public LocalTimeValue timeStarted;
        public DurationValue elapsedTime;

        static ProgressResult fromProgressEvent(ProgressEvent progressEvent) {
            var task = progressEvent.task();
            return new ProgressResult(task, progressEvent.jobId(), task.description());
        }

        static ProgressResult fromTaskWithDepth(Task task, JobId jobId, int depth) {
            var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
            return new ProgressResult(task, jobId, treeViewTaskName);
        }

        public ProgressResult(Task task, JobId jobId, String taskName) {
            var progressContainer = task.getProgress();

            this.jobId = jobId.asString();
            this.taskName = taskName;
            this.progress = StructuredOutputHelper.computeProgress(progressContainer.progress(), progressContainer.volume());
            this.progressBar = StructuredOutputHelper.progressBar(progressContainer.progress(), progressContainer.volume(), PROGRESS_BAR_LENGTH);
            this.status = task.status().name();
            this.timeStarted = localTimeValue(task);
            this.elapsedTime = computeElapsedTime(task);
        }

        private LocalTimeValue localTimeValue(Task task) {
            return LocalTimeValue.localTime(LocalTime.ofInstant(
                Instant.ofEpochMilli(task.startTime()),
                ZoneId.systemDefault()
            ));
        }

        private DurationValue computeElapsedTime(Task baseTask) {
            var finishTime = baseTask.finishTime();
            var finishTimeOrNow = finishTime != -1
                ? finishTime
                : System.currentTimeMillis();
            var elapsedTime = finishTimeOrNow - baseTask.startTime();
            var duration = Duration.ofMillis(elapsedTime);
            return DurationValue.duration(duration);
        }
    }

    public static class JobProgressVisitor extends DepthAwareTaskVisitor {

        private final JobId jobId;
        private final List<ProgressResult> progressRows;

        JobProgressVisitor(JobId jobId) {
            this.jobId = jobId;
            this.progressRows = new ArrayList<>();
        }

        List<ProgressResult> progressRows() {
            return this.progressRows;
        }

        @Override
        public void visitLeafTask(LeafTask leafTask) {
            addProgressRow(leafTask);
        }

        @Override
        public void visitIntermediateTask(Task task) {
            addProgressRow(task);
        }

        @Override
        public void visitIterativeTask(IterativeTask iterativeTask) {
            addProgressRow(iterativeTask);
        }

        private void addProgressRow(Task task) {
            progressRows.add(ProgressResult.fromTaskWithDepth(task, jobId, depth()));
        }
    }
}
