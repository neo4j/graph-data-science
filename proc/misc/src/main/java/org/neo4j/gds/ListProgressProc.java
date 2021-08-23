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

        public String id;
        public String taskName;
        public String stage;

        ProgressResult(ProgressEvent progressEvent) {
            this.id = progressEvent.jobId().asString();
            this.taskName = progressEvent.task().description();
            this.stage = computeStage(progressEvent.task());
        }

        private String computeStage(Task baseTask) {
            var subTaskCountingVisitor = new SubTaskCountingVisitor();
            baseTask.visit(subTaskCountingVisitor);

            String stageTemplate = "%s of %s";
            String stageResult;

            return subTaskCountingVisitor.containsOpenTask()
                ? formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), "n/a")
                : formatWithLocale(stageTemplate, subTaskCountingVisitor.numFinishedSubTasks(), subTaskCountingVisitor.numSubTasks());
        }
    }

    public static class SubTaskCountingVisitor implements TaskVisitor {

        private boolean containsOpenTask = false;
        private int numSubTasks = 0;
        private int numFinishedSubTasks = 0;

        int numSubTasks() {
            return numSubTasks;
        }

        int numFinishedSubTasks() {
            return numFinishedSubTasks;
        }

        boolean containsOpenTask() {
            return containsOpenTask;
        }

        @TestOnly
        void reset() {
            this.numSubTasks = 0;
            this.numFinishedSubTasks = 0;
            this.containsOpenTask = false;
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
                case OPEN:
                    containsOpenTask = true;
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
