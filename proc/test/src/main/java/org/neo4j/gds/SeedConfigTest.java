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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

public interface SeedConfigTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends SeedConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void testDefaultSeedPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.seedProperty());
    }

    @Test
    default void testSeedPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("seedProperty", "foo"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("foo", config.seedProperty());
    }

    @Test
    default void testEmptySeedPropertyValues() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("seedProperty", null));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.seedProperty());
    }

    @Test
    default void testTrimmedToNullSeedProperty() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("seedProperty", "  "));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.seedProperty());
    }

    @Test
    default void testSeedPropertyValidation() {
        String graphName = "loadedGraph";

        runQuery(graphDb(), "CREATE (:A {a: 1, b:2, c:3})");

        List<PropertyMapping> nodeProperties = Stream.of("a", "b", "c")
            .map(PropertyMapping::of)
            .collect(Collectors.toList());

        var graphCreateConfig = withNameAndNodeProjections(
            "",
            graphName,
            NodeProjections.create(Map.of(
                NodeLabel.of("A"), NodeProjection.of("A", PropertyMappings.of(nodeProperties))
                )
            )
        );

        GraphStore graphStore = graphLoader(graphCreateConfig).graphStore();
        GraphStoreCatalog.set(graphCreateConfig, graphStore);

        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            "seedProperty", "foo"
        ))).toMap();

        applyOnProcedure(proc -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> proc.compute(graphName, config)
            );
            assertThat(e.getMessage(), containsString("foo"));
            assertThat(e.getMessage(), containsString("[a, b, c]"));
        });

    }

    @Test
    default void shouldFailWithInvalidSeedProperty() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = emptyWithNameNative("", loadedGraphName);

        applyOnProcedure((proc) -> {
            GraphStore graphStore = graphLoader(graphCreateConfig).graphStore();

            GraphStoreCatalog.set(graphCreateConfig, graphStore);
            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                "seedProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = "Seed property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));

            if (supportsImplicitGraphCreate()) {
                Map<String, Object> implicitConfigMap = createMinimalImplicitConfig(mapWrapper).toMap();
                assertMissingProperty(error, () -> proc.compute(
                    implicitConfigMap,
                    Collections.emptyMap()
                ));
            }
        });
    }
}
