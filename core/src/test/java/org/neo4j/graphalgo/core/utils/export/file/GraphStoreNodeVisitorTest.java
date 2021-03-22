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
package org.neo4j.graphalgo.core.utils.export.file;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@GdlExtension
class GraphStoreNodeVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE" +
                              "  (a:A)" +
                              ", (b:A)" +
                              ", (c:B)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void shouldAddNodesToNodesBuilder() {
        NodeSchema nodeSchema = graphStore.schema().nodeSchema();
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .hasLabelInformation(true)
            .concurrency(1)
            .maxOriginalId(graphStore.nodeCount())
            .tracker(AllocationTracker.empty())
            .build();
        GraphStoreNodeVisitor nodeVisitor = new GraphStoreNodeVisitor(nodeSchema, nodesBuilder, false);
        graph.forEachNode(nodeId -> {
            nodeVisitor.id(nodeId);
            nodeVisitor.labels(graph.nodeLabels(nodeId).stream().map(NodeLabel::name).toArray(String[]::new));
            nodeVisitor.endOfEntity();
            return true;
        });

        var actualNodeMapping = nodesBuilder.build();
        var expectedNodeMapping = graph.nodeMapping();
        assertNodeMapping(actualNodeMapping, expectedNodeMapping);
    }

    static void assertNodeMapping(NodeMapping actual, NodeMapping expected) {
        assertThat(actual.nodeCount()).isEqualTo(expected.nodeCount());
        assertThat(actual.availableNodeLabels()).isEqualTo(expected.availableNodeLabels());
    }
}
