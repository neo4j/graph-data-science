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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AbstractProjections;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.fromProcedureConfig;

class GraphProjectFromCypherConfigTest {

    @ParameterizedTest
    @MethodSource("invalidKeys")
    void testThrowForInvalidProcedureConfigKeys(String invalidKey, AbstractProjections<?, ?> projections) {
        CypherMapWrapper config = CypherMapWrapper.empty()
            .withString(NODE_QUERY_KEY, ALL_NODES_QUERY)
            .withString(RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY)
            .withEntry(invalidKey, projections);

        assertThatThrownBy(() -> fromProcedureConfig("", config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid key");
    }

    @Test
    void omitCypherParametersFromToMap() {
        var config = GraphProjectFromCypherConfigImpl.builder()
            .graphName("g")
            .username("myUser")
            .parameters(Map.of("nodes", LongStream.range(0, 100).toArray()))
            .nodeQuery("UNWIND $nodes AS nodeId CREATE ({id: nodeId}")
            .relationshipQuery("some query")
            .build();

        assertThat(config.toMap()).contains(Map.entry("parameters", Set.of("nodes")));
    }

    static Stream<Arguments> invalidKeys() {
        return Stream.of(
            Arguments.of(GraphProjectFromStoreConfig.NODE_PROJECTION_KEY, NodeProjections.of()),
            Arguments.of(GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY, ImmutableRelationshipProjections.of()),
            Arguments.of(GraphProjectFromStoreConfig.NODE_PROPERTIES_KEY, NodeProjections.of())
        );
    }

}
