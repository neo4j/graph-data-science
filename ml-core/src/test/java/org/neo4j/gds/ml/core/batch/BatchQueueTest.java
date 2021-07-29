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
package org.neo4j.gds.ml.core.batch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class BatchQueueTest {
    @Test
    void shouldHandleVeryLargeBatches() {
        BatchQueue batchQueue = new BatchQueue(5L * Integer.MAX_VALUE, 1, 4);
        assertThat(batchQueue.pop().orElseThrow().size()).isEqualTo(Integer.MAX_VALUE);
        for (int i = 0; i < 4; i++) {
            assertThat(batchQueue.pop()).isPresent();
        }
        assertThat(batchQueue.pop()).isEmpty();
    }

    @Test
    void shouldRespectMinBatchSize() {
        BatchQueue batchQueue = new BatchQueue(100, 100, 4);
        int actualSize = batchQueue.pop().orElseThrow().size();
        assertThat(actualSize).isEqualTo(100);
        assertThat(batchQueue.pop()).isEmpty();
    }

    @Test
    void shouldNotGiveBatchLargerThanNodeCount() {
        BatchQueue batchQueue = new BatchQueue(100, 200, 4);
        int actualSize = batchQueue.pop().orElseThrow().size();
        assertThat(actualSize).isEqualTo(100);
        assertThat(batchQueue.pop()).isEmpty();
    }

    @Test
    void shouldDivideNodesAlmostEqually() {
        BatchQueue batchQueue = new BatchQueue(101, 1, 4);
        for (int i = 0; i < 3; i++) {
            assertThat(batchQueue.pop().orElseThrow().size()).isEqualTo(26);
        }
        assertThat(batchQueue.pop().orElseThrow().size()).isEqualTo(23);
        assertThat(batchQueue.pop()).isEmpty();
    }
}
