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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class TreeSetGapDetectorTest {

    @Test
    void shouldWork() {

        var labels = HugeLongArray.of(0, 1, 2, 3, 4, 5);
        var gapDetector = new TreeSetGapDetector(labels, labels.size());
        gapDetector.resetCounts();
        for (int d = 1; d < 6; ++d) {
            assertThat(gapDetector.isEmpty(d)).isFalse();
        }
        assertThat(gapDetector.isEmpty(0)).isFalse();
        assertThat(gapDetector.moveFrom(1, 1, 2)).isTrue();
        gapDetector.relabel(1);
        for (int d = 1; d < 6; ++d) {
            assertThat(gapDetector.isEmpty(d)).isTrue();
            assertThat(labels.get(d)).isEqualTo(6);
        }
        for (int i = 0; i < 6; ++i){
            labels.set(i, 3);
        }
        gapDetector.resetCounts();
        for (int d = 1; d < 6; ++d) {
            if (d==3){
                assertThat(gapDetector.isEmpty(d)).isFalse();
            }else {
                assertThat(gapDetector.isEmpty(d)).isTrue();
            }
        }




    }

}
