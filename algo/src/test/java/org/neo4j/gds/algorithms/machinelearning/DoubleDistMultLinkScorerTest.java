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
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseDoubleArrayArray;
import org.neo4j.gds.nodeproperties.DoubleArrayTestPropertyValues;

import static org.assertj.core.api.Assertions.assertThat;

class DoubleDistMultLinkScorerTest {
    @Test
    void shouldComputeEuclideanDistanceScore() {
        var propertyBuilder = HugeSparseDoubleArrayArray.builder(new double[]{0.0, 0.0, 0.0, 0.0});
        propertyBuilder.set(0, new double[]{1.0, 1.0, 1.0, 1.0});
        propertyBuilder.set(1, new double[]{2.0, 2.0, 2.0, 0.0});
        propertyBuilder.set(2, new double[]{3.0, 3.0, 3.0, 0.0});
        var hsdaa = propertyBuilder.build();
        DoubleArrayNodePropertyValues ddnpv = new DoubleArrayTestPropertyValues(hsdaa::get);

        LinkScorer linkScorer = new DoubleDistMultLinkScorer(ddnpv, DoubleArrayList.from(0.1, 0.1, 0.1, 0.1));
        linkScorer.init(0);

        assertThat(linkScorer.computeScore(1)).isCloseTo(0.6, Offset.offset(1e-02));
        assertThat(linkScorer.computeScore(2)).isCloseTo(0.9, Offset.offset(1e-02));

    }
}
