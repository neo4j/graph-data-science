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
package org.neo4j.gds.core;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphStoreAddNodeLabelTest {

    @GdlGraph(graphNamePrefix = "multiLabel")
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1})" +
        ", (b:A {p: 2})" +
        ", (c:A {p: 3})" +
        ", (d:B)";

    @Inject
    GraphStore multiLabelGraphStore;

    @Inject
    IdFunction multiLabelIdFunction;

    @GdlGraph(graphNamePrefix = "singleLabel")
    private static final String SINGLE_LABEL_DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1})" +
        ", (b:A {p: 2})" +
        ", (c:A {p: 3})";

    @Inject
    GraphStore singleLabelGraphStore;

    @Inject
    IdFunction singleLabelIdFunction;


    @Test
    void shouldAddLabelToMultiLabelGraphStore() {
        testAddingLabelToGraphStore(multiLabelGraphStore, multiLabelIdFunction);
    }

    @Test
    void shouldAddLabelToSingleLabelGraphStore() {
        testAddingLabelToGraphStore(singleLabelGraphStore, singleLabelIdFunction);
    }

    private void testAddingLabelToGraphStore(GraphStore graphStore, IdFunction idFunction) {
        // Arrange
        var b = idFunction.of("b");
        var c = idFunction.of("c");
        var newLabel = NodeLabel.of("Test");

        // Act
        graphStore.addNodeLabel(newLabel);
        graphStore.nodes().addNodeIdToLabel(b, newLabel);
        graphStore.nodes().addNodeIdToLabel(c, newLabel);

        // Assert
        // the node schema should have the node label
        var newLabelGraph = graphStore.getGraph(newLabel);
        assertThat(newLabelGraph.schema().nodeSchema().availableLabels())
            .containsExactly(newLabel);
        assertThat(newLabelGraph.availableNodeLabels())
            .containsExactly(newLabel);

        // The given nodes should have the node label
        assertThat(newLabelGraph.nodeCount()).isEqualTo(2L);
        assertThat(newLabelGraph.nodeCount(newLabel)).isEqualTo(2L);

        newLabelGraph.forEachNode(nodeId -> {
            assertThat(newLabelGraph.toOriginalNodeId(nodeId)).isIn(b, c);
            return true;
        });
    }
}
