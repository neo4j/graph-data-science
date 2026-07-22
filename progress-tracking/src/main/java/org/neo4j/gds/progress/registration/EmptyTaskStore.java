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
package org.neo4j.gds.progress.registration;

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tasks.Task;

import java.util.Optional;
import java.util.stream.Stream;

public enum EmptyTaskStore implements TaskStore {
    INSTANCE;

    @Override
    public void store(User user, JobId jobId, Task task) {}

    @Override
    public void remove(User user, JobId jobId) {}

    @Override
    public void markCompleted(User user, JobId jobId) {

    }

    @Override
    public Stream<StoredTask> query() {
        return Stream.empty();
    }

    @Override
    public Stream<StoredTask> query(JobId jobId) {
        return Stream.empty();
    }

    @Override
    public Stream<StoredTask> query(User user) {
        return Stream.empty();
    }

    @Override
    public Optional<StoredTask> lookup(User user, JobId jobId) {
        return Optional.empty();
    }

    @Override
    public long ongoingTaskCount() {
        return 0;
    }

    @Override
    public void addListener(TaskStoreListener listener) {}
}
