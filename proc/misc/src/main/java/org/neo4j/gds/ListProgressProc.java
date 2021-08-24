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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.core.utils.progress.ProgressEvent;
import org.neo4j.gds.core.utils.progress.ProgressEventStore;
import org.neo4j.gds.core.utils.progress.tasks.IterativeTask;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskVisitor;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalTimeValue;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ListProgressProc extends BaseProc {

    @Context
    public ProgressEventStore progress;

    @Procedure("gds.beta.listProgress")
    @Description("List progress events for currently running tasks.")
    public Stream<ProgressResult> listProgress() {
        return progress.query(username()).stream().map(ProgressResult::new);
    }

    @SuppressWarnings("unused")
    public static class ProgressResult {

        private static final String UNKNOWN = "n/a";

        public String id;
        public String taskName;
        public String stage;
        public String progress;
        public String status;
        public LocalTimeValue timeStarted;
        public DurationValue elapsedTime;

        ProgressResult(ProgressEvent progressEvent) {
            this.id = progressEvent.jobId().asString();
            var task = progressEvent.task();
            this.taskName = task.description();
            this.stage = computeStage(task);
            this.progress = computeProgress(task);
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
            String stageResult;

            return subTaskCountingVisitor.containsUnresolvedOpenTask()
                ? formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), UNKNOWN)
                : formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), subTaskCountingVisitor.numSubTasks());
        }

        private String computeProgress(Task baseTask) {
            var progressContainer = baseTask.getProgress();
            var volume = progressContainer.volume();
            var progress = progressContainer.progress();

            if (volume == Task.UNKNOWN_VOLUME) {
                return UNKNOWN;
            }

            var progressPercentage = (double) progress / (double) volume;
            var decimalFormat = new DecimalFormat("###.##%", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
            return decimalFormat.format(progressPercentage);
        }

        private LocalTimeValue localTimeValue(Task task) {
            return LocalTimeValue.localTime(LocalTime.ofInstant(
                Instant.ofEpochMilli(task.startTime()),
                ZoneId.systemDefault()
            ));
        }
    }

    public static class SubTaskCountingVisitor implements TaskVisitor {

        private boolean containsUnresolvedOpenTask = false;
        private int numSubTasks = 0;
        private int numFinishedSubTasks = 0;

        int numSubTasks() {
            return numSubTasks;
        }

        int numFinishedSubTasks() {
            return numFinishedSubTasks;
        }

        boolean containsUnresolvedOpenTask() {
            return containsUnresolvedOpenTask;
        }

        @TestOnly
        void reset() {
            this.numSubTasks = 0;
            this.numFinishedSubTasks = 0;
            this.containsUnresolvedOpenTask = false;
        }

        @Override
        public void visitLeafTask(LeafTask leafTask) {
            incrementCounters(leafTask);
        }

        @Override
        public void visitIntermediateTask(Task task) {
            incrementCounters(task);
            visitRecursively(task);
        }

        @Override
        public void visitIterativeTask(IterativeTask iterativeTask) {
            incrementCounters(iterativeTask);
            switch (iterativeTask.mode()) {
                case FIXED:
                case DYNAMIC:
                    visitRecursively(iterativeTask);
                    break;
                case OPEN:
                    if (iterativeTask.status() == Status.FINISHED) {
                        incrementCounters(iterativeTask);
                        containsUnresolvedOpenTask = false;
                    } else {
                        containsUnresolvedOpenTask = true;
                    }
                    break;
            }
        }

        private void visitRecursively(Task task) {
            task.subTasks().forEach(subTask -> subTask.visit(this));
        }

        private void incrementCounters(Task task) {
            numSubTasks++;
            if (task.status() == Status.FINISHED) {
                numFinishedSubTasks++;
            }
        }
    }
}
