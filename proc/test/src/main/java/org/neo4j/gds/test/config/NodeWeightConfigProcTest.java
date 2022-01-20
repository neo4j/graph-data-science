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
import org.neo4j.gds.config.NodeWeightConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.validation.Validator;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class NodeWeightConfigProcTest {

    public static <C extends AlgoBaseConfig & NodeWeightConfig> List<DynamicTest> defaultTest(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            defaultNodeWeightProperty(proc, config),
            emptyNodeWeightProperty(proc, config),
            validNodeWeightProperty(proc, config),
            validateNodeWeightProperty(proc, config),
            validateNodeWeightPropertyFilteredGraph(proc, config)
        );
    }

    public static <C extends AlgoBaseConfig & NodeWeightConfig> List<DynamicTest> mandatoryParameterTest(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            unspecifiedNodeWeightProperty(proc, config),
            validNodeWeightProperty(proc, config),
            validateNodeWeightProperty(proc, config),
            validateNodeWeightPropertyFilteredGraph(proc, config),
            trailingWhiteSpaceNodeWeightProperty(proc, config)
        );
    }

    private NodeWeightConfigProcTest() {}

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest validateNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        var graphStore = GdlFactory.of("(:A {a: 1})").build();
        return DynamicTest.dynamicTest("validateNodeWeightProperty", () -> {
            var validator = new Validator<>(proc.validationConfig());
            assertThatThrownBy(() -> validator.validateConfigWithGraphStore(
                    graphStore,
                    null,
                    proc.configParser().processInput(config.withString("nodeWeightProperty", "notA").toMap())
                )
            )
                .hasMessageContaining("Node weight property")
                .hasMessageContaining("notA")
                .hasMessageContaining("A")
                .hasMessageContaining("a");
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest validateNodeWeightPropertyFilteredGraph(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        var graphStore = GdlFactory.of("(a:Node), (b:Ignore {foo: 42}), (a)-[:T]->(b)").build();
        return DynamicTest.dynamicTest("validateNodeWeightPropertyFilteredGraph", () -> {
            var validator = new Validator<>(proc.validationConfig());
            assertThatThrownBy(() -> validator.validateConfigWithGraphStore(
                    graphStore,
                    null,
                    proc.configParser().processInput(config
                        .withString("nodeWeightProperty", "foo")
                        .withEntry("nodeLabels", List.of("Node"))
                        .toMap()
                    )
                )
            )
                .hasMessageContaining("Node weight property")
                .hasMessageContaining("foo")
                .hasMessageContaining("Node");
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest defaultNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("defaultNodeWeightProperty", () -> {
            var algoConfig = proc.configParser().processInput(config.toMap());
            assertThat(algoConfig.nodeWeightProperty()).isNull();
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest emptyNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("whitespaceNodeWeightProperty", () -> {
            var nodeWeightConfig = config.withString("nodeWeightProperty", "");
            var algoConfig = proc.configParser().processInput(nodeWeightConfig.toMap());
            assertThat(algoConfig.nodeWeightProperty()).isNull();
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest trailingWhiteSpaceNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("whitespaceNodeWeightProperty", () -> {
            var nodeWeightConfig = config.withString("nodeWeightProperty", " a");
            assertThatThrownBy(() -> proc.configParser().processInput(nodeWeightConfig.toMap()))
                .hasMessage("`nodeWeightProperty` must not end or begin with whitespace characters, but got ` a`.");
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest unspecifiedNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("unspecifiedNodeWeightProperty", () -> {
            assertThatThrownBy(() -> proc.configParser().processInput(config.withoutEntry("nodeWeightProperty").toMap()))
                .hasMessageContaining("nodeWeightProperty")
                .hasMessageContaining("mandatory");
        });
    }

    private static <C extends AlgoBaseConfig & NodeWeightConfig> DynamicTest validNodeWeightProperty(
        AlgoBaseProc<?, ?, C, ?> proc,
        CypherMapWrapper config
    ) {
        var graphStore = GdlFactory.of("(:A {nw: 1})").build();
        return DynamicTest.dynamicTest("validNodeWeightProperty", () -> {
            var nodeWeightConfig = config.withString("nodeWeightProperty", "nw");
            var algoConfig = proc.configParser().processInput(nodeWeightConfig.toMap());
            assertThat(algoConfig.nodeWeightProperty()).isEqualTo("nw");

            var validator = new Validator<>(proc.validationConfig());
            assertThatCode(() -> validator.validateConfigWithGraphStore(
                graphStore,
                null,
                algoConfig
            )).doesNotThrowAnyException();
        });
    }
}
