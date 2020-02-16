/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AbstractProjections;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.fromProcedureConfig;

class GraphCreateConfigFromCypherTest {

    @ParameterizedTest
    @MethodSource("invalidKeys")
    void testThrowForInvalidProcedureConfigKeys(String invalidKey, AbstractProjections<?> projections) {
        CypherMapWrapper config = CypherMapWrapper.empty()
            .withString(NODE_QUERY_KEY, ALL_NODES_QUERY)
            .withString(RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY)
            .withEntry(invalidKey, projections);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> fromProcedureConfig("", config)
        );

        assertThat(ex.getMessage(), containsString("Invalid key"));
    }

    static Stream<Arguments> invalidKeys() {
        return Stream.of(
            Arguments.of(GraphCreateFromStoreConfig.NODE_PROJECTION_KEY, NodeProjections.of()),
            Arguments.of(GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY, RelationshipProjections.of())
        );
    }

}