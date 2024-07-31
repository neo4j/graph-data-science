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

import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.DepthAwareTaskVisitor;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class JobProgressVisitor extends DepthAwareTaskVisitor {
    private final JobId jobId;
    private final String username;
    private final List<ProgressResult> progressRows;

    JobProgressVisitor(JobId jobId, String username) {
        this.jobId = jobId;
        this.username = username;
        this.progressRows = new ArrayList<>();
    }

    Stream<ProgressResult> progressRowsStream() {
        return progressRows.stream();
    }

    @Override
    public void visit(Task task) {
        progressRows.add(ProgressResult.fromTaskWithDepth(username, task, jobId, depth()));
    }
}
