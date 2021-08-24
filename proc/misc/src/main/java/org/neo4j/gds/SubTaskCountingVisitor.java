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
import org.neo4j.gds.core.utils.progress.tasks.IterativeTask;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskVisitor;

public class SubTaskCountingVisitor implements TaskVisitor {

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
