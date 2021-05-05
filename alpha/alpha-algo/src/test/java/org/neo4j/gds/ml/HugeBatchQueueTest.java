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
package org.neo4j.gds.ml;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class HugeBatchQueueTest {
    @Test
    void test() {
        HugeLongArray data = HugeLongArray.of(3, 6, 2, 3, 6, 2, 76, 3 , 2, 6, 7, 42, 43);
        var hugeBatchQueue = new HugeBatchQueue(data, 5);
        var b1 = hugeBatchQueue.pop();
        assertThat(b1).isPresent();
        assertThat(b1.get().nodeIds()).containsExactly(3L, 6L, 2L, 3L, 6L);
        var b2 = hugeBatchQueue.pop();
        assertThat(b2).isPresent();
        assertThat(b2.get().nodeIds()).containsExactly(2L, 76L, 3L, 2L, 6L);
        var b3 = hugeBatchQueue.pop();
        assertThat(b3).isPresent();
        assertThat(b3.get().nodeIds()).containsExactly(7L, 42L, 43L);
    }
}
