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
package org.neo4j.gds.core.utils.progress.tasks;

import java.util.List;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class IterativeTask extends Task {

    public enum Mode {
        // upper bound but can terminate early
        DYNAMIC,
        // unbounded
        OPEN,
        // upper bound and will execute exactly n times
        FIXED
    }

    private final Supplier<List<Task>> subTasksSupplier;
    private final Mode mode;
    private final int maxIterations;

    /**
     * @param subTasks Requires an unrolled list of subtasks for modes DYNAMIC and FIXED.
     */
    IterativeTask(
        String description,
        List<Task> subTasks,
        Supplier<List<Task>> subTasksSupplier,
        Mode mode
    ) {
        super(description, subTasks);
        this.subTasksSupplier = subTasksSupplier;
        this.mode = mode;
        this.maxIterations = subTasks().size() / subTasksSupplier.get().size();
    }

    @Override
    public Progress getProgress() {
        var progress = super.getProgress();

        if (mode == Mode.OPEN && status() != Status.FINISHED) {
            return ImmutableProgress.of(progress.progress(), UNKNOWN_VOLUME);
        }

        return progress;
    }

    @Override
    protected Task nextSubTaskAfterValidation() {
        var maybeRunningTask = subTasks().stream().filter(t -> t.status() == Status.RUNNING).findFirst();
        if (maybeRunningTask.isPresent()) {
            throw new IllegalStateException(formatWithLocale(
                "Cannot move to next subtask, because subtask `%s` is still running",
                maybeRunningTask.get().description()
            ));
        }

        var maybeNextSubtask = subTasks().stream().filter(t -> t.status() == Status.PENDING).findFirst();

        if (maybeNextSubtask.isPresent()) {
            return maybeNextSubtask.get();
        } else if (mode == Mode.OPEN) {
            var newIterationTasks = subTasksSupplier.get();
            subTasks().addAll(newIterationTasks);
            return newIterationTasks.get(0);
        } else {
            throw new IllegalStateException("No more pending subtasks");
        }
    }

    @Override
    public void finish() {
        super.finish();
        subTasks().forEach(t -> {
            if (t.status() == Status.PENDING) {
                t.cancel();
            }
        });
    }

    public int currentIteration() {
        return (int) subTasks().stream().filter(t -> t.status() == Status.FINISHED).count() / subTasksSupplier
            .get()
            .size();
    }

    public Mode mode() {
        return this.mode;
    }

    int maxIterations() {
        return this.maxIterations;
    }

    @Override
    public void visit(TaskVisitor taskVisitor) {
        taskVisitor.visitIterativeTask(this);
    }
}
