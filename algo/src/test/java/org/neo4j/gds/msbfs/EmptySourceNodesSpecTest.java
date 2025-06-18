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
package org.neo4j.gds.msbfs;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class EmptySourceNodesSpecTest {

    @ParameterizedTest
    @CsvSource(
        {
        "true, 0 ,0",
        "false,1, 2"
        }
    )
    void shouldSetUpProperly(boolean allowStartNode, long expectedSeen1, long expectedSeen2){

        HugeLongArray visit = HugeLongArray.newArray(100);
        HugeLongArray seen = HugeLongArray.newArray(100);

        var empty = new EmptySourceNodesSpec(5, 2);

        long touchedNode1 =  5;
        long touchedNode2 = 6;
        empty.setUp(visit,seen,allowStartNode);
        for (int i=0; i<100; ++i){
            if (i == touchedNode1 || i == touchedNode2) continue;
            assertThat(visit.get(i)).isEqualTo(0L);
            assertThat(seen.get(i)).isEqualTo(0L);
        }
        assertThat(visit.get(touchedNode1)).isEqualTo(1L << 0);
        assertThat(visit.get(touchedNode2)).isEqualTo(1L << 1);

        assertThat(seen.get(touchedNode1)).isEqualTo(expectedSeen1);
        assertThat(seen.get(touchedNode2)).isEqualTo(expectedSeen2);

    }

}
