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

import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.TaskTraversal;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class OperationsProcedureFacade {
    private final RequestScopedDependencies requestScopedDependencies;

    public OperationsProcedureFacade(RequestScopedDependencies requestScopedDependencies) {
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public Stream<ProgressResult> listProgress(String jobId) {
        return jobId.isBlank()
            ? jobsSummaryView()
            : jobDetailView(jobId);
    }

    private Stream<ProgressResult> jobsSummaryView() {
        var taskStore = requestScopedDependencies.getTaskStore();
        var user = requestScopedDependencies.getUser();

        if (user.isAdmin()) {
            return taskStore.query().map(ProgressResult::fromTaskStoreEntry);
        } else {
            return taskStore.query(user.getUsername()).map(ProgressResult::fromTaskStoreEntry);
        }
    }

    private Stream<ProgressResult> jobDetailView(String jobIdAsString) {
        var jobId = new JobId(jobIdAsString);

        var taskStore = requestScopedDependencies.getTaskStore();
        var user = requestScopedDependencies.getUser();

        if (user.isAdmin()) {
            var progressResults = taskStore
                .query(jobId)
                .flatMap(this::jobProgress)
                .collect(Collectors.toList());

            if (progressResults.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "No task with job id `%s` was found.",
                    jobIdAsString
                ));
            }

            return progressResults.stream();
        } else {
            return taskStore.query(user.getUsername(), jobId).map(this::jobProgress).orElseThrow(
                () -> new IllegalArgumentException(formatWithLocale(
                    "No task with job id `%s` was found.",
                    jobIdAsString
                ))
            );
        }
    }

    private Stream<ProgressResult> jobProgress(TaskStore.UserTask userTask) {
        var jobProgressVisitor = new JobProgressVisitor(userTask.jobId(), userTask.username());
        TaskTraversal.visitPreOrderWithDepth(userTask.task(), jobProgressVisitor);
        return jobProgressVisitor.progressRowsStream();
    }
}
