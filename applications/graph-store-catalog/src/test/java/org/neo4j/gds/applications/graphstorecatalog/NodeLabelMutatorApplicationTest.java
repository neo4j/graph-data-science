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
package org.neo4j.gds.applications.graphstorecatalog;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Map;

import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

@ExtendWith(SoftAssertionsExtension.class)
@GdlExtension
class NodeLabelMutatorApplicationTest {
    @SuppressWarnings("unused")
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:A {longProperty: 1, floatProperty: 15.5})" +
            ", (b:A {longProperty: 2, floatProperty: 23})" +
            ", (c:A {longProperty: 3, floatProperty: 18.3})" +
            ", (d:B)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "all")
    private static final String ALL_DB_CYPHER =
        "CREATE" +
            "  (a{longProperty: 1})" +
            ", (b{longProperty: 2})" +
            ", (c{longProperty: 3})" +
            ", (d{longProperty:0})";

    @SuppressWarnings("WeakerAccess")
    @Inject
    private GraphStore allGraphStore;

    @SuppressWarnings("WeakerAccess")
    @Inject
    private IdFunction allIdFunction;

    @Test
    void mutateNodeLabelMultiLabelProjection(SoftAssertions assertions) {
        var graphStoreCatalogService = new GraphStoreCatalogService();
        var configuration = GraphProjectConfig.emptyWithName("user", "graph");
        graphStoreCatalogService.set(configuration, graphStore, ResultStore.EMPTY);
        var service = new NodeLabelMutatorApplication();

        var result = service.compute(
            graphStore,
            GraphName.parse("graph"),
            "TestLabel",
            MutateLabelConfig.of(Map.of("nodeFilter", "n:A AND n.longProperty > 1")),
            NodeFilterParser.parseAndValidate(graphStore, "n:A AND n.longProperty > 1")
        );

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
        var graphStoreCatalogService = new GraphStoreCatalogService();
        var configuration = GraphProjectConfig.emptyWithName("user", "graph");
        graphStoreCatalogService.set(configuration, graphStore, ResultStore.EMPTY);
        var nodeLabelMutatorService = new NodeLabelMutatorApplication();

        var result = nodeLabelMutatorService.compute(
            graphStore,
            GraphName.parse("graph"),
            "TestLabel",
            MutateLabelConfig.of(Map.of("nodeFilter", "n.floatProperty <= 19.0")),
            NodeFilterParser.parseAndValidate(graphStore, "n.floatProperty <= 19.0")
        );

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
        var graphStoreCatalogService = new GraphStoreCatalogService();
        var configuration = GraphProjectConfig.emptyWithName("user", "graph");
        graphStoreCatalogService.set(configuration, allGraphStore, ResultStore.EMPTY);
        var nodeLabelMutatorService = new NodeLabelMutatorApplication();

        var result = nodeLabelMutatorService.compute(
            allGraphStore,
            GraphName.parse("graph"),
            "TestLabel",
            MutateLabelConfig.of(Map.of("nodeFilter", "n.longProperty <= 2")),
            NodeFilterParser.parseAndValidate(graphStore, "n.longProperty <= 2")
        );

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

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
