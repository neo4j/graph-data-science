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
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Set;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

@GdlExtension
class GraphStoreNodeVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE" +
                              "  (a:A {prop1: 42L, prop2: [1.0, 2.0]})" +
                              ", (b:A {prop1: 43L, prop2: [3.0, 4.0]})" +
                              ", (c:B {prop3: 13.37D})";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void shouldAddNodesToNodesBuilder() {
        NodeSchema nodeSchema = graphStore.schema().nodeSchema();
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
            .concurrency(1)
            .maxOriginalId(graphStore.nodeCount())
            .nodeCount(graph.nodeCount())
            .tracker(AllocationTracker.empty())
            .build();
        GraphStoreNodeVisitor nodeVisitor = new GraphStoreNodeVisitor(nodeSchema, nodesBuilder);
        graph.forEachNode(nodeId -> {
            nodeVisitor.id(nodeId);
            Set<NodeLabel> nodeLabels = graph.nodeLabels(nodeId);
            nodeVisitor.labels(nodeLabels.stream().map(NodeLabel::name).toArray(String[]::new));
            var propertyKeys = graphStore.nodePropertyKeys(nodeLabels);
            for (String propertyKey : propertyKeys) {
                nodeVisitor.property(propertyKey, graph.nodeProperties(propertyKey).value(nodeId).asObject());
            }
            nodeVisitor.endOfEntity();
            return true;
        });

        var nodeMappingAndProperties = nodesBuilder.build();
        var nodeMapping = nodeMappingAndProperties.nodeMapping();
        var nodeProperties = nodeMappingAndProperties.unionNodePropertiesOrThrow();
        var relationships = GraphFactory.emptyRelationships(nodeMapping, AllocationTracker.empty());
        HugeGraph actualGraph = GraphFactory.create(
            nodeMapping,
            nodeSchema,
            nodeProperties,
            RelationshipType.ALL_RELATIONSHIPS,
            relationships,
            AllocationTracker.empty());
        assertGraphEquals(graph, actualGraph);
    }
}
