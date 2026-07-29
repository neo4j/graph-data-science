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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.logging.Log;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LocalTaskRegistryFactory implements TaskRegistryFactory {
    private final Log log;
    private final TaskStore taskStore;
    private final User user;

    public LocalTaskRegistryFactory(Log log, TaskStore taskStore, User user) {
        this.log = log;
        this.taskStore = taskStore;
        this.user = user;
    }

    @Override
    public TaskRegistry newInstance(JobId jobId) {
        var tasks = taskStore.lookup(user, jobId);

        var possibleActiveTask = tasks.stream()
            .filter(storedTask -> ActiveStatuses.Statuses.contains(storedTask.task().status()))
            .findFirst();

        if (possibleActiveTask.isPresent()) {
            var firstActiveTask = possibleActiveTask.get().task();

            throw new IllegalArgumentException(
                formatWithLocale(
                    "There's already a job running with jobId '%s'. Existing Job: (desc: `%s`, status: `%s`)",
                    jobId.asString(),
                    firstActiveTask.description(),
                    firstActiveTask.status()
                )
            );
        }

        return new TaskRegistry(user, taskStore, jobId);
    }

    @Override
    public TaskRegistry attach(JobId jobId) {
        var tasks = taskStore.lookup(user, jobId);

        if (tasks.isEmpty()) {
            log.warn("cannot attach to job '" + jobId.asString() + "'");

            // if you got here, it is because something went wrong.
            // that this corner case exists is an artifact of the design, c'est la vie. go fix that.

            // so you are here either because of programmer error - you are attaching but shouldn't be.
            // or, you could have got here because of a race condition,
            // tasks completed and cleaned up before you arrived,
            // maybe the user was extremely aggressive with retention period.

            // we can't tell, and thus we are left with what to do. throw an exception, throw toys out the pram?
            // tempting, but no, that would be disproportionately disruptive for users.
            // so we fall back to newing up, like nothing happened; and hope we fix the design in future
            log.warn("falling back to overriding job '" + jobId.asString() + "'");
            return newInstance(jobId);
        }

        // So, at this point, we know a job with some tasks exists. We do not care about the state of it.
        // It could be going on, in which case: great! We start more work on that job in parallel
        // It could have just finished: great! This is the next work in the sequence for this job
        // It could be some old, stale, unrelated job: not great! But we can't tell.
        // The latter is the crux if the problem with this solution, but as noted,
        // categorically solving it requires _a lot_ of changes.

        return new TaskRegistry(user, taskStore, jobId);
    }

    /**
     * We use this in TaskRegistryFactoryServiceTest to see if the taskStore is shared.
     * <p>
     * This is a design smell, and we ought to change things, but I am loath to side questing right now.
     */
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    /**
     * This is to make spotbugs happy.
     * We only use these in tests btw, so I think it makes sense to use those Apache Commons tools
     */
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
