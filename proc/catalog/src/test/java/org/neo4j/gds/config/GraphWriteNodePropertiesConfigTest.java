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
package org.neo4j.gds.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class GraphWriteNodePropertiesConfigTest {

    @GdlGraph(graphNamePrefix = "propertiesSubset")
    private static final String GDL_PROPERTIES_SUBSET =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3})" +
            ", (e:B {nodeProp1: 4})" +
            ", (f:B {nodeProp1: 5})";

    @Inject
    private GraphStore propertiesSubsetGraphStore;

    @ParameterizedTest(name = "{1}")
    @MethodSource("nodeLabels")
    void validNodeLabels(Object nodeLabelsToValidate, String displayName) {
        var config = GraphWriteNodePropertiesConfig.of(
            "propertiesSubsetGraph",
            List.of("nodeProp1", "nodeProp2"),
            nodeLabelsToValidate,
            CypherMapWrapper.empty()
        );

        var validNodeLabels = config.validNodeLabels(propertiesSubsetGraphStore);

        assertThat(validNodeLabels)
            .hasSize(1)
            .satisfiesExactly(nodeLabel -> {
                    assertThat(propertiesSubsetGraphStore.nodePropertyKeys(nodeLabel))
                        .as("NodeLabel `%s` does not contain all requested properties", nodeLabel.name())
                        .containsExactlyInAnyOrder("nodeProp1", "nodeProp2");
                }
            )
            .as("Valid node labels are only the ones containing all of the requested node properties")
            .containsExactly(NodeLabel.of("A"));
    }

    public static Stream<Arguments> nodeLabels() {
        return Stream.of(
            Arguments.of("*", "Implicit `all labels`"),
            Arguments.of(List.of("A", "B"), "Explicit `all labels`")
        );
    }

    @MethodSource("inputs")
    @ParameterizedTest
    void concurrencies(Map<String, Object> inputs, int expectedConcurrency, int expectedWriteConcurrency) {
        var map = CypherMapWrapper.create(inputs);
        var config = GraphWriteNodePropertiesConfig.of("g", List.of("a"), List.of("A"), map);

        assertEquals(expectedConcurrency, config.concurrency());
        assertEquals(expectedWriteConcurrency, config.writeConcurrency());
    }

    private static Stream<Arguments> inputs() {
        return Stream.of(
            Arguments.of(Map.of(), ConcurrencyConfig.DEFAULT_CONCURRENCY, ConcurrencyConfig.DEFAULT_CONCURRENCY),
            Arguments.of(Map.of("concurrency", 2), 2, 2),
            Arguments.of(Map.of("writeConcurrency", 3), ConcurrencyConfig.DEFAULT_CONCURRENCY, 3),
            Arguments.of(Map.of("concurrency", 2, "writeConcurrency", 3), 2, 3)
        );
    }

}
