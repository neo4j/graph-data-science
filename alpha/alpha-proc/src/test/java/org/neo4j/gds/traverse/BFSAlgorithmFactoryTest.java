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
package org.neo4j.gds.traverse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.config.SourceNodeConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BFSAlgorithmFactoryTest {

    @ParameterizedTest
    @CsvSource({"10_000,100_000,564268,2004268", "100_000,1_000_000,5639418,20039418"})
    void testMemoryEstimation(long nodeCount, long relationshipCount, long expectedMin, long expectedMax) {
        var algorithmFactory = new BFSAlgorithmFactory();

        var userInput = CypherMapWrapper.create(Map.of(SourceNodeConfig.SOURCE_NODE_KEY, 0));

        var memoryEstimation = algorithmFactory.memoryEstimation(BfsStreamConfig.of(userInput));
        var dimensions = GraphDimensions.builder().nodeCount(nodeCount).relCountUpperBound(relationshipCount).build();
        var actual = memoryEstimation.estimate(dimensions, 1).memoryUsage();

        assertThat(actual.min).isEqualTo(expectedMin);
        assertThat(actual.max).isEqualTo(expectedMax);
    }
}
