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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventQueueTracker;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressEventQueueTrackerTest {

    @Test
    void should() {
        var queue = new ConcurrentLinkedQueue<LogEvent>();
        var username = AuthSubject.ANONYMOUS.username();
        var tracker = new ProgressEventQueueTracker(queue, username);
        tracker.release();
        assertThat(queue.size()).isOne();
        var event = queue.remove();
        assertThat(event.isEndOfStream()).isTrue();
        assertThat(event.username()).isEqualTo(username);
        assertThat(event.message()).isEqualTo(LogEvent.NO_MESSAGE);
        assertThat(event.taskName()).isEqualTo(LogEvent.NO_TASK_NAME);
    }
}
