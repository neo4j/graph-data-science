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

import org.neo4j.gds.applications.operations.ResultRenderer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.TaskTraversal;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class DefaultResultRenderer implements ResultRenderer<ProgressResult> {
    private final JobId jobId;

    DefaultResultRenderer(JobId jobId) {
        this.jobId = jobId;
    }

    @Override
    public Stream<ProgressResult> renderAdministratorView(Stream<TaskStore.UserTask> results) {
        var progressResultStream = results.flatMap(this::jobProgress);

        var progressResults = progressResultStream.collect(Collectors.toList());

        if (progressResults.isEmpty()) throw createException();

        return progressResults.stream();
    }

    @Override
    public Stream<ProgressResult> render(Optional<TaskStore.UserTask> results) {
        return results
            .map(this::jobProgress)
            .orElseThrow(this::createException);
    }

    private IllegalArgumentException createException() {
        return new IllegalArgumentException(
            formatWithLocale(
                "No task with job id `%s` was found.",
                jobId.asString()
            )
        );
    }

    private Stream<ProgressResult> jobProgress(TaskStore.UserTask userTask) {
        var jobProgressVisitor = new JobProgressVisitor(userTask.jobId(), userTask.username());
        TaskTraversal.visitPreOrderWithDepth(userTask.task(), jobProgressVisitor);
        return jobProgressVisitor.progressRowsStream();
    }
}
