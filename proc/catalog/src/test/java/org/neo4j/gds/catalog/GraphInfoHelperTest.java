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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class GraphInfoHelperTest {

    @GdlGraph
    private static final String GRAPH =
        "CREATE" +
        "  (a)-->(b)" +
        ", (a)-->(c)" +
        ", (a)-->(d)" +
        ", (b)-->(c)" +
        ", (b)-->(d)" +
        ", (c)-->(d)";

    @Inject
    private Graph graph;

    @Test
    void degreeDistribution() {
        var actual = GraphInfoHelper.degreeDistribution(graph, TerminationFlag.RUNNING_TRUE);
        var expected = Map.of(
            "min", 0L,
            "max", 3L,
            "mean", 1.5D,
            "p50", 1L,
            "p75", 2L,
            "p90", 3L,
            "p95", 3L,
            "p99", 3L,
            "p999", 3L
        );
        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @ParameterizedTest
    @MethodSource("densitySource")
    void density(long nodeCount, long relationshipCount, double expectedDensity) {
        assertEquals(expectedDensity, GraphInfoHelper.density(nodeCount, relationshipCount));
    }

    @Test
    void graphBasedDensity() {
        assertEquals(GraphInfoHelper.density(graph), 0.5);
    }

    private static Stream<Arguments> densitySource() {
        return Stream.of(
            Arguments.of(0, 10, 0),
            Arguments.of(10, 9, 0.1)
        );
    }
}
