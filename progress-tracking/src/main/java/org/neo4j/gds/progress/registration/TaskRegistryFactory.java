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
import org.neo4j.gds.logging.Log;

public interface TaskRegistryFactory {
    /**
     * If you know you are the first or only thing doing tasks for a job id, use this method.
     * You use this at the very start of a job. You will need to know in your code, what the situation is.
     * For example, you use this at the point where you construct an algorithm.
     *
     * @throws IllegalArgumentException if a job with that id is already active (finished jobs get overwritten)
     */
    TaskRegistry newInstance(JobId jobId);

    /**
     * If you know you are _not_ the first, and therefore not the only, thing doing tasks for a job id, use this method.
     * Something came before you. You are composing onto it.
     * You are most likely a write mode tacking on to an algorithm.
     */
    TaskRegistry attach(JobId jobId);

    static TaskRegistryFactory local(Log log, TaskStore taskStore, User user) {
        return new LocalTaskRegistryFactory(log, taskStore, user);
    }

    static TaskRegistryFactory empty() {
        return EmptyTaskRegistryFactory.INSTANCE;
    }
}
