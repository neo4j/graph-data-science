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

import java.util.Set;
import java.util.stream.Stream;

public interface TaskStore {
    void store(User user, JobId jobId, Task task);

    void remove(User user, JobId jobId);

    void markCompleted(User user, JobId jobId);

    /**
     * @return all tasks across users and jobs - we hope only admins call this
     */
    Stream<StoredTask> query();

    /**
     * Oddly enough there seems to be a use case - certainly usage - of getting task information, for a given job id,
     * _regardless of user_. Some might call that information leakage or a back door.
     * Could it not be resolved by querying using the default user?
     */
    Stream<StoredTask> query(JobId jobId);

    Stream<StoredTask> query(User user);

    /**
     * @return stored tasks in task id order, i.e. original insertion order
     */
    Set<StoredTask> lookup(User user, JobId jobId);

    default Stream<StoredTask> queryRunning() {
        return query().filter(storedTask -> storedTask.task().status().isOngoing());
    }

    long ongoingTaskCount();

    void addListener(TaskStoreListener listener);
}
