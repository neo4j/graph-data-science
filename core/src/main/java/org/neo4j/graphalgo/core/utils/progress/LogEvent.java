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
package org.neo4j.graphalgo.core.utils.progress;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.OptionalDouble;

@ValueClass
public interface LogEvent {

    String NO_TASK_NAME = "";
    String NO_MESSAGE = "";

    String username();

    JobId jobId();

    String taskName();

    String message();

    OptionalDouble progress();

    @Value.Default
    @Value.Parameter(false)
    default boolean isEndOfStream() {
        return false;
    }

    static LogEvent endOfStreamEvent(String username, JobId jobId) {
        return ImmutableLogEvent.builder()
            .username(username)
            .taskName(NO_TASK_NAME)
            .message(NO_MESSAGE)
            .jobId(jobId)
            .isEndOfStream(true)
            .build();
    }
}
