/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionUtilsTest {

    public class TestTask implements Runnable {

        public final long start;
        public final long end;

        TestTask(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {

        }

        @Override
        public String toString() {
            return String.format("(%d, %d)", start, end);
        }
    }

    @Test
    void testAlignment() {
        long alignTo = 64;
        long nodeCount = 200;
        int concurrency = 2;

        Collection<TestTask> tasks = PartitionUtils.numberAlignedPartitioning(
            TestTask::new,
            concurrency,
            nodeCount,
            alignTo
            );

        assertEquals(2, tasks.size());
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 0 && t.end == 127),
            String.format("Expected task with start %d and end %d, but found %s", 0, 127, tasks)
        );
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 128 && t.end == 200),
            String.format("Expected task with start %d and end %d, but found %s", 128, 200, tasks)
        );
    }

}