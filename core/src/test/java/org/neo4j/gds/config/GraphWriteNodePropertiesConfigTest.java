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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.config.GraphWriteNodePropertiesConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphWriteNodePropertiesConfigTest {

    @MethodSource("inputs")
    @ParameterizedTest
    void concurrencies(Map<String, Object> inputs, int expectedConcurrency, int expectedWriteConcurrency) {
        var map = CypherMapWrapper.create(inputs);
        var config = GraphWriteNodePropertiesConfig.of("tester", "g", List.of("a"), List.of("A"), map);

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
