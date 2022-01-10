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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConfigurableSeedConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface ConfigurableSeedConfigTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends ConfigurableSeedConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void testDefaultSeedPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.seedProperty());
    }

    @Test
    default void testSeedPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(seedPropertyKeyOverride(), "foo"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("foo", config.seedProperty());
    }

    @Test
    default void testEmptySeedPropertyValues() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(seedPropertyKeyOverride(), null));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.seedProperty());
    }

    @Test
    default void failOnBlankPropertyName() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(seedPropertyKeyOverride(), "  "));
        assertThatThrownBy(() -> createConfig(createMinimalConfig(mapWrapper)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not end or begin with whitespace characters");
    }

    @Test
    default void testSeedPropertyValidation() {
        runQuery(graphDb(), "CREATE (:A {a: 1, b:2, c:3})");
        var graphName = "graph";
        List<PropertyMapping> nodeProperties = Stream.of("a", "b", "c").map(PropertyMapping::of)
            .collect(Collectors.toList());

        GraphProjectFromStoreConfig graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
            "",
            graphName,
            NodeProjections.single(NodeLabel.of("A"), NodeProjection.of("A", PropertyMappings.of(nodeProperties))),
            allRelationshipsProjection()
        );

        GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
        GraphStoreCatalog.set(graphProjectConfig, graphStore);


        Map<String, Object> config = createMinimalConfig(CypherMapWrapper
            .empty()
            .withString(seedPropertyKeyOverride(), "foo")).toMap();

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
        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            "",
            loadedGraphName,
            allRelationshipsProjection()
        );

        applyOnProcedure((proc) -> {
            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();

            GraphStoreCatalog.set(graphProjectConfig, graphStore);
            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                seedPropertyKeyOverride(),
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = formatWithLocale(
                "`%s`: `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found",
                seedPropertyKeyOverride()
            );
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));
        });
    }

    default String seedPropertyKeyOverride() {
        return "seedProperty";
    }

    private RelationshipProjections allRelationshipsProjection() {
        return this instanceof OnlyUndirectedTest<?, ?, ?>
            ? RelationshipProjections.ALL_UNDIRECTED
            : RelationshipProjections.ALL;
    }
}
