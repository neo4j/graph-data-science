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
package org.neo4j.gds.core.compression.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BlockStatisticsTest {

    @Test
    void testFastPFORHeuristic() throws Exception {
        long[] values = new long[] {
            0b10,
            0b10,
            0b1,
            0b10,
            0b100110,
            0b10,
            0b1,
            0b11,
            0b10,
            0b100000,
            0b10,
            0b110100,
            0b10,
            0b11,
            0b11,
            0b1
        };

        try (BlockStatistics blockStatistics = new BlockStatistics()) {
            blockStatistics.record(values, 0, values.length);

            var maxBits = blockStatistics.maxBits();
            assertThat(maxBits.maxValue()).isEqualTo(6);

            var bestMaxDiffBits = blockStatistics.bestMaxDiffBits();
            assertThat(bestMaxDiffBits.maxValue()).isEqualTo(4);

            var exceptions = blockStatistics.exceptions();
            assertThat(exceptions.maxValue()).isEqualTo(3);
        }
    }

}
