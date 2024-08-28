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
package org.neo4j.gds.algorithms.machinelearning;

import com.carrotsearch.hppc.DoubleArrayList;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.hsa.HugeSparseFloatArrayArray;
import org.neo4j.gds.nodeproperties.FloatArrayTestPropertyValues;

import static org.assertj.core.api.Assertions.assertThat;

class FloatDistMultLinkScorerTest {
    @Test
    void shouldComputeEuclideanDistanceScore() {
        var propertyBuilder = HugeSparseFloatArrayArray.builder(new float[]{0.0f, 0.0f, 0.0f, 0.0f});
        propertyBuilder.set(0, new float[]{1.0f, 1.0f, 1.0f, 1.0f});
        propertyBuilder.set(1, new float[]{2.0f, 2.0f, 2.0f, 0.0f});
        propertyBuilder.set(2, new float[]{3.0f, 3.0f, 3.0f, 0.0f});
        var hsfaa = propertyBuilder.build();
        var ddnpv = new FloatArrayTestPropertyValues(hsfaa::get);

        LinkScorer linkScorer = new FloatDistMultLinkScorer(ddnpv, DoubleArrayList.from(0.1, 0.1, 0.1, 0.1));
        linkScorer.init(0);

        assertThat(linkScorer.computeScore(1)).isCloseTo(0.6, Offset.offset(1e-02));
        assertThat(linkScorer.computeScore(2)).isCloseTo(0.9, Offset.offset(1e-02));

    }
}
