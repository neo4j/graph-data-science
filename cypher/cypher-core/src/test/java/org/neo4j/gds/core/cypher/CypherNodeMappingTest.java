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
package org.neo4j.gds.core.cypher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class CypherIdMapTest {

    @GdlGraph
    static String GRAPH = "CREATE" +
                          "  (a:A)" +
                          ", (b:B)" +
                          ", (c:C)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    CypherIdMap idMap;

    @BeforeEach
    void setup() {
        this.idMap = new CypherIdMap(graphStore.nodes());
    }

    @Test
    void shouldAddNewNodeLabels() {
        var newNodeLabel = NodeLabel.of("new");
        this.idMap.addNodeLabel(newNodeLabel);
        assertThat(this.idMap.availableNodeLabels()).contains(newNodeLabel);
        assertThat(this.graphStore.nodes().availableNodeLabels()).doesNotContain(newNodeLabel);
    }

    @Test
    void shouldAddNewLabelsToNode() {
        var newNodeLabel = NodeLabel.of("new");
        var nodeA = idFunction.of("a");
        this.idMap.addLabelToNode(nodeA, newNodeLabel);
        assertThat(this.idMap.nodeLabels(nodeA)).containsExactlyInAnyOrder(NodeLabel.of("A"), newNodeLabel);
        assertThat(this.idMap.hasLabel(nodeA, newNodeLabel)).isTrue();
        assertThat(this.idMap.hasLabel(nodeA, NodeLabel.of("B"))).isFalse();
    }

    @Test
    void shouldAddAlreadyExistingLabelsToNode() {
        var existingLabel = NodeLabel.of("B");
        var nodeA = idFunction.of("a");
        this.idMap.addLabelToNode(nodeA, existingLabel);
        assertThat(this.idMap.nodeLabels(nodeA)).containsExactlyInAnyOrder(NodeLabel.of("A"), existingLabel);
        assertThat(this.idMap.hasLabel(nodeA, existingLabel)).isTrue();
        assertThat(this.idMap.hasLabel(nodeA, NodeLabel.of("C"))).isFalse();
    }

    @Test
    void shouldIterateThroughNewAndExistingAddedNodeLabels() {
        var newNodeLabel = NodeLabel.of("new");
        var existingLabel = NodeLabel.of("B");
        var nodeA = idFunction.of("a");
        this.idMap.addLabelToNode(nodeA, newNodeLabel);
        this.idMap.addLabelToNode(nodeA, existingLabel);

        var nodeLabels = new HashSet<NodeLabel>();
        this.idMap.forEachNodeLabel(nodeA, nodeLabels::add);
        assertThat(nodeLabels).containsExactlyInAnyOrder(NodeLabel.of("A"), newNodeLabel, existingLabel);
    }

    @Test
    void shouldRemoveLabelFromNode() {
        var newNodeLabel = NodeLabel.of("new");
        this.idMap.addNodeLabel(newNodeLabel);
        this.idMap.addLabelToNode(idFunction.of("a"), newNodeLabel);
        this.idMap.addLabelToNode(idFunction.of("b"), newNodeLabel);

        assertThat(this.idMap.hasLabel(idFunction.of("b"), newNodeLabel)).isTrue();

        this.idMap.removeLabelFromNode(idFunction.of("b"), newNodeLabel);

        assertThat(this.idMap.hasLabel(idFunction.of("a"), newNodeLabel)).isTrue();
        assertThat(this.idMap.hasLabel(idFunction.of("b"), newNodeLabel)).isFalse();
    }
}
