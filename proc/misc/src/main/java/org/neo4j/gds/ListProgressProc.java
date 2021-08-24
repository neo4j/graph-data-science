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
    public Stream<JobProgressRow> listProgressDetail(
        @Name(value = "jobId") String jobId
    ) {
        return new JobProgressResult(progress.query(username(), JobId.fromString(jobId))).stream();
    }

    @SuppressWarnings("unused")
    public static class ProgressResult {

        public String id;
        public String taskName;
        public String stage;
        public String progress;
        public String status;
        public LocalTimeValue timeStarted;
        public DurationValue elapsedTime;

        ProgressResult(ProgressEvent progressEvent) {
            var task = progressEvent.task();
            var progress = task.getProgress();

            this.id = progressEvent.jobId().asString();
            this.taskName = task.description();
            this.stage = computeStage(task);
            this.progress = StructuredOutputHelper.computeProgress(progress.progress(), progress.volume());
            this.status = task.status().name();
            this.timeStarted = localTimeValue(task);
            this.elapsedTime = computeElapsedTime(task);
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

        private String computeStage(Task baseTask) {
            var subTaskCountingVisitor = new SubTaskCountingVisitor();
            baseTask.visit(subTaskCountingVisitor);

            String stageTemplate = "%s of %s";

            return subTaskCountingVisitor.containsUnresolvedOpenTask()
                ? formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), UNKNOWN)
                : formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), subTaskCountingVisitor.numSubTasks());
        }

        private LocalTimeValue localTimeValue(Task task) {
            return LocalTimeValue.localTime(LocalTime.ofInstant(
                Instant.ofEpochMilli(task.startTime()),
                ZoneId.systemDefault()
            ));
        }
    }

    public static class JobProgressResult {

        private final List<JobProgressRow> jobProgressRows;

        JobProgressResult(ProgressEvent progressEvent) {
            var task = progressEvent.task();
            var jobProgressVisitor = new JobProgressVisitor();
            TaskTraversal.visitPreOrderWithDepth(task, jobProgressVisitor);
            this.jobProgressRows = jobProgressVisitor.progressRows();
        }

        public Stream<JobProgressRow> stream() {
            return this.jobProgressRows.stream();
        }
    }

    public static class JobProgressRow {
        public String taskName;
        public String progressBar;
        public String progress;

        public JobProgressRow(String taskName, String progressBar, String progress) {
            this.taskName = taskName;
            this.progressBar = progressBar;
            this.progress = progress;
        }

        static JobProgressRow fromTaskWithDepth(Task task, int depth) {
            var progressContainer = task.getProgress();
            var volume = progressContainer.volume();
            var progress = progressContainer.progress();

            var treeViewTaskName = StructuredOutputHelper.treeViewDescription(task.description(), depth);
            var progressBar = StructuredOutputHelper.progressBar(progress, volume, PROGRESS_BAR_LENGTH);
            var progressString = StructuredOutputHelper.computeProgress(progress, volume);
            return new JobProgressRow(treeViewTaskName, progressBar, progressString);
        }

    }

    public static class JobProgressVisitor extends DepthAwareTaskVisitor {

        private final List<JobProgressRow> progressRows;

        JobProgressVisitor() {
            this.progressRows = new ArrayList<>();
        }

        List<JobProgressRow> progressRows() {
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
            progressRows.add(JobProgressRow.fromTaskWithDepth(task, depth));
        }
    }
}
