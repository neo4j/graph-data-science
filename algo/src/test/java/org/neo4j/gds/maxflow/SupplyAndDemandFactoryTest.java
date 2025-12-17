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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@GdlExtension
class SupplyAndDemandFactoryTest {

    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0}]->(d),
                (b)-[:R {w: 3.0}]->(a),
                (c)-[:R {w: 2.0}]->(a),
                (c)-[:R {w: 0.0}]->(b),
                (d)-[:R {w: 5.0}]->(e)
            """;

    @Inject
    private TestGraph graph;

    @Test
    void testCreateWithListSourceAndListTargetNodes() {
        var a = graph.toMappedNodeId("a");
        var b = graph.toMappedNodeId("b");
        var c = graph.toMappedNodeId("c");
        var d = graph.toMappedNodeId("d");
        var aOriginal = graph.toOriginalNodeId("a");
        var bOriginal = graph.toOriginalNodeId("b");
        var cOriginal = graph.toOriginalNodeId("c");
        var dOriginal = graph.toOriginalNodeId("d");

        var sourceNodes = new ListInputNodes(List.of(aOriginal, bOriginal));
        var targetNodes = new ListInputNodes(List.of(cOriginal, dOriginal));

        var result = SupplyAndDemandFactory.create(graph, sourceNodes, targetNodes);

        assertThat(result.getLeft()).containsExactlyInAnyOrder(new NodeWithValue(a, 4.0), new NodeWithValue(b, 3.0));
        assertThat(result.getRight()).containsExactlyInAnyOrder(new NodeWithValue(c, 7.0), new NodeWithValue(d, 7.0));
    }

    @Test
    void testCreateWithListSourceAndMapTargetNodes() {
        var a = graph.toMappedNodeId("a");
        var b = graph.toMappedNodeId("b");
        var c = graph.toMappedNodeId("c");
        var e = graph.toMappedNodeId("e");


        var aOriginal = graph.toOriginalNodeId("a");
        var bOriginal = graph.toOriginalNodeId("b");
        var cOriginal = graph.toOriginalNodeId("c");
        var eOriginal = graph.toOriginalNodeId("e");

        // Arrange
        var sourceNodes = new ListInputNodes(List.of(aOriginal, bOriginal));
        var targetNodes = new MapInputNodes(Map.of(cOriginal, 5.0, eOriginal, 8.0));

        // Act
        var result = SupplyAndDemandFactory.create(graph, sourceNodes, targetNodes);

        // Assert
        assertThat(result.getLeft()).containsExactlyInAnyOrder(new NodeWithValue(a, 4.0), new NodeWithValue(b, 3.0));
        assertThat(result.getRight()).containsExactlyInAnyOrder(new NodeWithValue(c, 5.0), new NodeWithValue(e, 8.0));
    }

    @Test
    void testCreateWithMapSourceAndListTargetNodes() {
        var a = graph.toMappedNodeId("a");
        var c = graph.toMappedNodeId("c");
        var d = graph.toMappedNodeId("d");
        var e = graph.toMappedNodeId("e");

        var aOriginal = graph.toOriginalNodeId("a");
        var cOriginal = graph.toOriginalNodeId("c");
        var dOriginal = graph.toOriginalNodeId("d");
        var eOriginal = graph.toOriginalNodeId("e");

        var sourceNodes = new MapInputNodes(Map.of(aOriginal, 1.2, dOriginal, 10.0));
        var targetNodes = new ListInputNodes(List.of(cOriginal, eOriginal));

        var result = SupplyAndDemandFactory.create(graph, sourceNodes, targetNodes);

        assertThat(result.getLeft()).containsExactlyInAnyOrder(new NodeWithValue(a, 1.2), new NodeWithValue(d, 10.0));
        assertThat(result.getRight()).containsExactlyInAnyOrder(new NodeWithValue(c, 11.2), new NodeWithValue(e, 11.2));
    }

    @Test
    void testCreateWithMapSourceAndMapTargetNodes() {
        var a = graph.toMappedNodeId("a");
        var c = graph.toMappedNodeId("c");
        var d = graph.toMappedNodeId("d");
        var e = graph.toMappedNodeId("e");

        var aOriginal = graph.toOriginalNodeId("a");
        var cOriginal = graph.toOriginalNodeId("c");
        var dOriginal = graph.toOriginalNodeId("d");
        var eOriginal = graph.toOriginalNodeId("e");

        var sourceNodes = new MapInputNodes(Map.of(aOriginal, 1.0, cOriginal, 3.0));
        var targetNodes = new MapInputNodes(Map.of(dOriginal, 5.1, eOriginal, 9.0));

        var result = SupplyAndDemandFactory.create(graph, sourceNodes, targetNodes);

        assertThat(result.getLeft()).containsExactlyInAnyOrder(new NodeWithValue(a, 1.0), new NodeWithValue(c, 3.0));
        assertThat(result.getRight()).containsExactlyInAnyOrder(new NodeWithValue(d, 5.1), new NodeWithValue(e, 9.0));
    }

    @Test
    void shouldThrowForParsingBothNodeCapacityAndMap() {

        var aOriginal = graph.toOriginalNodeId("a");
        var eOriginal = graph.toOriginalNodeId("e");

        var sourceNodes = new MapInputNodes(Map.of(aOriginal, 1.0));
        var targetNodes = new MapInputNodes(Map.of(eOriginal, 9.0));

        var props = mock(NodePropertyValues.class);
        when(props.doubleValue(anyLong())).thenReturn(20d);
        assertThatThrownBy( () ->SupplyAndDemandFactory.create(graph, props, sourceNodes, targetNodes))
            .hasMessage("Passing source node constraints via both `nodeCapacityProperty` and map is not accepted");

        var sourcesNodes2 = new ListInputNodes(List.of(aOriginal));

        assertThatThrownBy( () ->SupplyAndDemandFactory.create(graph, props, sourcesNodes2, targetNodes))
            .hasMessage("Passing target node constraints via both `nodeCapacityProperty` and map is not accepted");
    }

    @Test
    void shouldThrowForNegativeNodePropertyValues() {

        var aOriginal = graph.toOriginalNodeId("a");
        var eOriginal = graph.toOriginalNodeId("e");

        var props = mock(NodePropertyValues.class);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(aOriginal)))).thenReturn(20d);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(eOriginal)))).thenReturn(-20d);

        var badSourceNodes = new ListInputNodes(List.of(eOriginal));
        var goodSourceNodes = new ListInputNodes(List.of(aOriginal));
        var targetNodes = new ListInputNodes(List.of(eOriginal));

        assertThatThrownBy( () ->SupplyAndDemandFactory.create(graph, props, badSourceNodes, targetNodes))
            .hasMessage("Source node values must be positive, but found a negative value.");

        assertThatThrownBy( () ->SupplyAndDemandFactory.create(graph, props, goodSourceNodes, targetNodes))
            .hasMessage("Target node values must be positive, but found a negative value.");
    }

    @Test
    void shouldCreateFromNodePropertyValues() {

        var aOriginal = graph.toOriginalNodeId("a");
        var bOriginal = graph.toOriginalNodeId("b");
        var cOriginal = graph.toOriginalNodeId("c");
        var eOriginal = graph.toOriginalNodeId("e");

        var props = mock(NodePropertyValues.class);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(aOriginal)))).thenReturn(10d);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(bOriginal)))).thenReturn(Double.NaN);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(cOriginal)))).thenReturn(Double.NaN);
        when(props.doubleValue(ArgumentMatchers.eq(graph.toMappedNodeId(eOriginal)))).thenReturn(20d);

        var sourceNodes = new ListInputNodes(List.of(aOriginal,bOriginal));
        var targetNodes = new ListInputNodes(List.of(cOriginal,eOriginal));

        var output = SupplyAndDemandFactory.create(graph,props,sourceNodes,targetNodes);
        assertThat(output.getLeft()[0].value()).isEqualTo( 10d);
        assertThat(output.getLeft()[1].value()).isEqualTo( 3d);
        assertThat(output.getRight()[0].value()).isEqualTo(13d);
        assertThat(output.getRight()[1].value()).isEqualTo(20d);

    }

}
