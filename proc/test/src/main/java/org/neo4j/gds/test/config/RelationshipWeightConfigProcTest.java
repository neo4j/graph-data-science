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
package org.neo4j.gds.test.config;

import org.junit.jupiter.api.DynamicTest;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.test.config.ConfigProcTestHelpers.GRAPH_NAME;

public final class RelationshipWeightConfigProcTest {
    private RelationshipWeightConfigProcTest() {}

    public static <C extends AlgoBaseConfig & RelationshipWeightConfig> List<DynamicTest> allTheTests(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            assertRelationshipWeightPropertyCanBeConfigured(proc, config),
            assertDefaultRelationshipWeightPropertyIsNull(proc, config),
            assertBlankRelationshipWeightPropertyIsNull(proc, config),
            assertEmptyRelationshipWeightPropertyIsNull(proc, config),
            assertRelationshipWeightPropertyIsValid(proc, config),
            assertRelationshipWeightPropertyIsInvalid(proc, config)
        );
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertRelationshipWeightPropertyCanBeConfigured(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("relationshipWeightProperty", () -> {
            var relationshipWeightConfig = config.withString("relationshipWeightProperty", "foo");
            var algoConfig = proc.newConfig(GRAPH_NAME, relationshipWeightConfig);
            assertThat(algoConfig.relationshipWeightProperty()).isEqualTo("foo");
        });
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertDefaultRelationshipWeightPropertyIsNull(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("defaultRelationshipWeightProperty", () -> {
            var algoConfig = proc.newConfig(GRAPH_NAME, config);
            assertThat(algoConfig.relationshipWeightProperty()).isNull();
        });
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertBlankRelationshipWeightPropertyIsNull(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("blankRelationshipWeightProperty", () -> {
            var relationshipWeightConfig = config.withString("relationshipWeightProperty", "  ");
            assertThatThrownBy(() -> proc.newConfig(GRAPH_NAME, relationshipWeightConfig)).hasMessage(
                "`relationshipWeightProperty` must not end or begin with whitespace characters, but got `  `.");
        });
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertEmptyRelationshipWeightPropertyIsNull(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("emptyRelationshipWeightProperty", () -> {
            var configAsMap = config.toMap();
            configAsMap.put("relationshipWeightProperty", null);
            var relationshipWeightConfig = CypherMapWrapper.create(configAsMap);
            var algoConfig = proc.newConfig(GRAPH_NAME, relationshipWeightConfig);
            assertThat(algoConfig.relationshipWeightProperty()).isNull();
        });
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertRelationshipWeightPropertyIsValid(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        var graphStore = GdlFactory.of("()-[:A {rrw: 4}]->()-[:A {rw: 3}]->(), ()-[:A {rw: 2}]->(), ()-[:A {rw: 1}]->()").build().graphStore();
        return DynamicTest.dynamicTest("validRelationshipWeightProperty", () -> {
            var relationshipWeightConfig = config.withString("relationshipWeightProperty", "rw");
            var algoConfig = proc.newConfig(GRAPH_NAME, relationshipWeightConfig);
            assertThat(algoConfig.relationshipWeightProperty()).isEqualTo("rw");
            assertThatCode(() -> proc.validateConfigWithGraphStore(
                graphStore,
                null,
                algoConfig
            )).doesNotThrowAnyException();
        });
    }

    private static <C extends AlgoBaseConfig & RelationshipWeightConfig> DynamicTest assertRelationshipWeightPropertyIsInvalid(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        var graphStore = GdlFactory.of("()-[:A {foo: 1}]->()").build().graphStore();
        return DynamicTest.dynamicTest("invalidRelationshipWeightProperty", () -> {
            var relationshipWeightConfig = config.withString("relationshipWeightProperty", "bar");
            var algoConfig = proc.newConfig(GRAPH_NAME, relationshipWeightConfig);
            assertThat(algoConfig.relationshipWeightProperty()).isEqualTo("bar");
            assertThatThrownBy(() -> proc.validateConfigWithGraphStore(
                graphStore,
                null,
                algoConfig
            ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Relationship weight property `bar` not found in relationship types ['A']. Properties existing on all relationship types: ['foo']");
        });
    }
}
