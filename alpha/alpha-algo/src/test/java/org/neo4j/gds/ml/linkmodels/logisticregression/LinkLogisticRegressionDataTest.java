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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryUsage;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class LinkLogisticRegressionDataTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 1000})
    void shouldEstimateCorrectly(int numberOfFeatures) {
        var foo = RelationshipType.of("FOO");
        var dimensions = GraphDimensions
            .builder()
            .nodeCount(24L)
            .relationshipCounts(Map.of(foo, 7331L))
            .build();
        var memoryEstimation = LinkLogisticRegressionData.memoryEstimation(numberOfFeatures).estimate(dimensions, 5000);
        long expectedArrayMemory = 16 + numberOfFeatures * 8;
        long expected = MemoryUsage.sizeOfInstance(ImmutableLinkLogisticRegressionData.class) + expectedArrayMemory;
        long actual = memoryEstimation.memoryUsage().max;
        assertThat(actual).isEqualTo(memoryEstimation.memoryUsage().min);
        assertThat(actual).isEqualTo(expected);
    }
}
