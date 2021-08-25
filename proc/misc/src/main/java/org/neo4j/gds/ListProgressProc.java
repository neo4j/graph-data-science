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

import static org.neo4j.gds.StructuredOutputHelper.UNKNOWN;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ListProgressProc extends BaseProc {

    static final int PROGRESS_BAR_LENGTH = 10;

    @Context
    public ProgressEventStore progress;

    @Procedure("gds.beta.listProgress")
    @Description("List progress events for currently running tasks.")
    public Stream<ProgressResult> listProgress() {
        return progress.query(username()).stream().map(ProgressResult::new);
    }

    @Procedure("gds.beta.listProgressDetail")
    @Description("List detailed progress events for the specified job id.")
    public Stream<JobProgressResult> listProgressDetail(
        @Name(value = "jobId") String jobId
    ) {
        var progressEvent = progress.query(username(), JobId.fromString(jobId));
        var task = progressEvent.task();
        var jobProgressVisitor = new JobProgressVisitor();
        TaskTraversal.visitPreOrderWithDepth(task, jobProgressVisitor);
        return jobProgressVisitor.progressRows().stream();
    }

    public static class CommonProgressResult {
        public String progress;
        public String status;
        public LocalTimeValue timeStarted;
        public DurationValue elapsedTime;

        public CommonProgressResult(Task task) {
            var progressContainer = task.getProgress();

            this.progress = StructuredOutputHelper.computeProgress(progressContainer.progress(), progressContainer.volume());
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

    @SuppressWarnings("unused")
    public static class ProgressResult extends CommonProgressResult {

        public String id;
        public String taskName;
        public String stage;

        ProgressResult(ProgressEvent progressEvent) {
            super(progressEvent.task());

            var task = progressEvent.task();
            this.id = progressEvent.jobId().asString();
            this.taskName = task.description();
            this.stage = computeStage(task);
        }

        private String computeStage(Task baseTask) {
            var subTaskCountingVisitor = new SubTaskCountingVisitor();
            baseTask.visit(subTaskCountingVisitor);

            String stageTemplate = "%s of %s";

            return subTaskCountingVisitor.containsUnresolvedOpenTask()
                ? formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), UNKNOWN)
                : formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), subTaskCountingVisitor.numSubTasks());
        }
    }

    public static class JobProgressResult extends CommonProgressResult {
        public String taskName;
        public String progressBar;

        public JobProgressResult(Task task, String taskName, String progressBar) {
            super(task);
            this.taskName = taskName;
            this.progressBar = progressBar;
        }

        static JobProgressResult fromTaskWithDepth(Task task, int depth) {
            var progressContainer = task.getProgress();
            var volume = progressContainer.volume();
            var progress = progressContainer.progress();

            var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
            var progressBar = StructuredOutputHelper.progressBar(progress, volume, PROGRESS_BAR_LENGTH);
            return new JobProgressResult(task, treeViewTaskName, progressBar);
        }

    }

    public static class JobProgressVisitor extends DepthAwareTaskVisitor {

        private final List<JobProgressResult> progressRows;

        JobProgressVisitor() {
            this.progressRows = new ArrayList<>();
        }

        List<JobProgressResult> progressRows() {
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
            progressRows.add(JobProgressResult.fromTaskWithDepth(task, depth()));
        }
    }
}
