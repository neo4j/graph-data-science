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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

@ExtendWith(SoftAssertionsExtension.class)
@GdlExtension
class NodeLabelMutatorTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:A {longProperty: 1, floatProperty: 15.5})" +
            ", (b:A {longProperty: 2, floatProperty: 23})" +
            ", (c:A {longProperty: 3, floatProperty: 18.3})" +
            ", (d:B)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "all")
    private static final String ALL_DB_CYPHER =
        "CREATE" +
            "  (a{longProperty: 1})" +
            ", (b{longProperty: 2})" +
            ", (c{longProperty: 3})" +
            ", (d{longProperty:0})";

    @Inject
    GraphStore allGraphStore;

    @Inject
    IdFunction allIdFunction;


    // Only check two scenarios, the rest are covered in NodeFilterParserTest.

    @ParameterizedTest
    @ValueSource(
        strings = {
            "n.floatProperty > 10",
            "n.longProperty <= 19.6",
        }
    )
    void shouldFailOnIncompatiblePropertyAndValue(String nodeFilter) {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder().build();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> NodeLabelMutator.mutateNodeLabel(
                "graph",
                "TestLabel",
                Map.of("nodeFilter", nodeFilter),
                executionContext
            ).findFirst())
            .havingCause()
            .withMessageContaining("Semantic errors while parsing expression")
            .withMessageContaining("Incompatible types");

    }

    @Test
    void mutateNodeLabelMultiLabelProjection(SoftAssertions assertions) {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder().build();

        var resultList = NodeLabelMutator.mutateNodeLabel(
            "graph",
            "TestLabel",
            Map.of("nodeFilter", "n:A AND n.longProperty > 1"),
            executionContext
        ).collect(
            Collectors.toList());

        assertions.assertThat(resultList).hasSize(1);

        var result = resultList.get(0);

        assertions.assertThat(result.nodeCount)
            .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
            .isEqualTo(4L);

        assertions.assertThat(result.nodeLabel)
            .as("The specified node label should be present in the result")
            .isEqualTo("TestLabel");

        assertions.assertThat(result.nodeLabelsWritten)
            .as("There should be two nodes having the new label in the in-memory graph")
            .isEqualTo(2L);

        assertions.assertThat(graphStore.nodeLabels())
            .as("The in-memory graph should contain the new label")
            .contains(NodeLabel.of("TestLabel"));

        var modifiedGraph = graphStore.getGraph(NodeLabel.of("TestLabel"));
        assertions.assertThat(modifiedGraph.nodeCount())
            .as("There should be two  nodes")
            .isEqualTo(2L);

        var nodePropertyValues = modifiedGraph.nodeProperties("longProperty");

        modifiedGraph.forEachNode(
            nodeId -> {
                assertions.assertThat(modifiedGraph.toOriginalNodeId(nodeId))
                    .asInstanceOf(LONG)
                    .as("Only nodes `b` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("b"),
                        idFunction.of("c")
                    );

                assertions.assertThat(nodePropertyValues.longValue(nodeId))
                    .asInstanceOf(LONG)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(1);
                return true;
            }
        );

    }

    @Test
    void shouldWorkWithFloatProperties(SoftAssertions assertions) {

        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder().build();

        var resultList = NodeLabelMutator.mutateNodeLabel(
            "graph",
            "TestLabel",
            Map.of("nodeFilter", "n.floatProperty <= 19.0"),
            executionContext
        ).collect(
            Collectors.toList());

        assertions.assertThat(resultList).hasSize(1);

        var result = resultList.get(0);

        assertions.assertThat(result.nodeCount)
            .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
            .isEqualTo(4L);

        assertions.assertThat(result.nodeLabel)
            .as("The specified node label should be present in the result")
            .isEqualTo("TestLabel");

        assertions.assertThat(result.nodeLabelsWritten)
            .as("There should be two nodes having the new label in the in-memory graph")
            .isEqualTo(2L);

        assertions.assertThat(graphStore.nodeLabels())
            .as("The in-memory graph should contain the new label")
            .contains(NodeLabel.of("TestLabel"));

        var modifiedGraph = graphStore.getGraph(NodeLabel.of("TestLabel"));
        assertions.assertThat(modifiedGraph.nodeCount())
            .as("There should be two  nodes")
            .isEqualTo(2L);

        var nodePropertyValues = modifiedGraph.nodeProperties("floatProperty");

        modifiedGraph.forEachNode(
            nodeId -> {
                assertions.assertThat(modifiedGraph.toOriginalNodeId(nodeId))
                    .asInstanceOf(LONG)
                    .as("Only nodes `a` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("a"),
                        idFunction.of("c")
                    );

                assertions.assertThat(nodePropertyValues.doubleValue(nodeId))
                    .asInstanceOf(DOUBLE)
                    .as("The node property should match the applied filter")
                    .isLessThanOrEqualTo(19d);
                return true;
            }
        );

    }

    @Test
    void mutateNodeLabelStarProjection(SoftAssertions assertions) {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), allGraphStore);
        var executionContext = executionContextBuilder().build();

        var resultList = NodeLabelMutator.mutateNodeLabel(
            "graph",
            "TestLabel",
            Map.of("nodeFilter", "n.longProperty <= 2"),
            executionContext
        ).collect(
            Collectors.toList());

        assertions.assertThat(resultList).hasSize(1);

        var result = resultList.get(0);

        assertions.assertThat(result.nodeCount)
            .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
            .isEqualTo(4L);

        assertions.assertThat(result.nodeLabel)
            .as("The specified node label should be present in the result")
            .isEqualTo("TestLabel");

        assertions.assertThat(result.nodeLabelsWritten)
            .as("There should be three nodes having the new label in the in-memory graph")
            .isEqualTo(3);

        assertions.assertThat(allGraphStore.nodeLabels())
            .as("The in-memory graph should contain the new label")
            .contains(NodeLabel.of("TestLabel"));

        var modifiedGraph = allGraphStore.getGraph(NodeLabel.of("TestLabel"));
        assertions.assertThat(modifiedGraph.nodeCount())
            .as("There should be three  nodes")
            .isEqualTo(3L);

        var nodePropertyValues = modifiedGraph.nodeProperties("longProperty");

        modifiedGraph.forEachNode(
            nodeId -> {
                assertions.assertThat(modifiedGraph.toOriginalNodeId(nodeId))
                    .asInstanceOf(LONG)
                    .as("Nodes `a` and `b` and `d` should have the `TestLabel` applied.")
                    .isIn(
                        allIdFunction.of("a"),
                        allIdFunction.of("b"),
                        allIdFunction.of("d")
                    );

                assertions.assertThat(nodePropertyValues.longValue(nodeId))
                    .asInstanceOf(LONG)
                    .as("The node property should match the applied filter")
                    .isLessThanOrEqualTo(2);
                return true;
            }
        );

    }


    private ImmutableExecutionContext.Builder executionContextBuilder() {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseId())
            .dependencyResolver(EmptyDependencyResolver.INSTANCE)
            .returnColumns(ProcedureReturnColumns.EMPTY)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .username("user")
            .terminationMonitor(TerminationMonitor.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .log(Neo4jProxy.testLog())
            .isGdsAdmin(false);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

}
