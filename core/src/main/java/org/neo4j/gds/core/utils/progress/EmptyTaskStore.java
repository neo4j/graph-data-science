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
package org.neo4j.gds.core.utils.progress;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public enum EmptyTaskStore implements TaskStore {
    INSTANCE;

    @Override
    public void store(String username, JobId jobId, Task task) {}

    @Override
    public void remove(String username, JobId jobId) {}

    @Override
    public @NotNull Map<JobId, Task> query(String username) {
        return Map.of();
    }

    @Override
    public Optional<Task> query(String username, JobId jobId) {
        return Optional.empty();
    }

    @Override
    public Stream<Task> taskStream() {
        return Stream.empty();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public long taskCount() {
        return 0;
    }

}
