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
package org.neo4j.graphalgo.gdl;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GdlFactoryTest {

    private Graph fromGdl(String gdl) {
        return GdlFactory.of(gdl).build().graphStore().getUnion();
    }

    @Test
    void testInvariants() {
        var graph = fromGdl("(a { foo: 42, bar: NaN }), (b { foo: 23, bar: 42.0d }), (a)-[{ weight: 42 }]->(b)");
        assertThat(graph.nodeCount()).isEqualTo(2);
        assertThat(graph.relationshipCount()).isEqualTo(1);
        assertThat(graph.availableNodeProperties()).isEqualTo(Set.of("foo", "bar"));
        assertThat(graph.hasRelationshipProperty()).isTrue();
        assertThat(graph.isUndirected()).isFalse();
        assertThat(graph.isEmpty()).isFalse();
    }

    @Test
    void testNodeLabels() {
        Graph graph = fromGdl("(a:A),(b:B1:B2),(c)");
        assertThat(graph.nodeCount()).isEqualTo(3);
        assertThat(graph.relationshipCount()).isEqualTo(0);
        var expectedLabels = Set.of("A", "B1", "B2").stream()
            .map(NodeLabel::new)
            .collect(Collectors.toSet());
        assertThat(graph.availableNodeLabels()).isEqualTo(expectedLabels);
    }

    @Test
    void testRelationshipTypes() {
        var gdlFactory = GdlFactory.of("(a)-[:REL1]->(b)-[:REL2]->(c)");
        var graphStore = gdlFactory.build().graphStore();
        var rel1Graph = graphStore.getGraph(RelationshipType.of("REL1"));
        assertThat(rel1Graph.relationshipCount()).isEqualTo(1);
        rel1Graph.forEachRelationship(gdlFactory.nodeId("a"), (sourceNodeId, targetNodeId) -> {
            assertThat(targetNodeId).isEqualTo(gdlFactory.nodeId("b"));
            return true;
        });

        var rel2Graph = graphStore.getGraph(RelationshipType.of("REL2"));
        assertThat(rel2Graph.relationshipCount()).isEqualTo(1);
        rel2Graph.forEachRelationship(gdlFactory.nodeId("b"), (sourceNodeId, targetNodeId) -> {
            assertThat(targetNodeId).isEqualTo(gdlFactory.nodeId("c"));
            return true;
        });
    }

    @Test
    void testRelationshipProperties() {
        var gdlFactory = GdlFactory.of(("(a)-[:REL { foo: 42, bar: 1337, baz: 84 }]->(b)"));
        var graphStore = gdlFactory.build().graphStore();

        assertRelationshipProperty(gdlFactory, 42.0,
            graphStore.getGraph(RelationshipType.of("REL"), Optional.of("foo"))
        );
        assertRelationshipProperty(gdlFactory, 1337.0,
            graphStore.getGraph(RelationshipType.of("REL"), Optional.of("bar"))
        );
        assertRelationshipProperty(gdlFactory, 84.0,
            graphStore.getGraph(RelationshipType.of("REL"), Optional.of("baz"))
        );
    }

    private void assertRelationshipProperty(GdlFactory gdlFactory, double expected, RelationshipIterator graph) {
        graph.forEachRelationship(gdlFactory.nodeId("a"), Double.NaN, (sourceNodeId, targetNodeId, property) -> {
            assertThat(property).isEqualTo(expected);
            return true;
        });
    }

    @Test
    void testCompatibleListProperties() {
        var graph = fromGdl("({f1: [1L, 3L, 3L, 7L], f2: [1.0D, 3.0D, 3.0D, 7.0D], f3: [1.0F, 3.0F, 3.0F, 7.0F]})");
        assertThat(graph.nodeProperties("f1").longArrayValue(0)).isEqualTo(new long[]{1, 3, 3, 7});
        assertThat(graph.nodeProperties("f2").doubleArrayValue(0)).isEqualTo(new double[]{1, 3, 3, 7});
        assertThat(graph.nodeProperties("f3").floatArrayValue(0)).isEqualTo(new float[]{1, 3, 3, 7});
    }

    @Test
    void testIncompatibleListProperties() {
        assertThatThrownBy(() -> fromGdl("({f1: [1, 3, 3, 7]})"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Integer");
    }

    @Test
    void testMixedListProperties() {
        assertThatThrownBy(() -> fromGdl("({f1: [4L, 2.0D, 4.2]})"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("[Long, Double, Float]");
    }

    @Test
    void testForAllNodes() {
        var graph = fromGdl("({w:1}),({w:2}),({w:3})");
        List<Double> nodeProps = Lists.newArrayList();
        graph.forEachNode(nodeId -> nodeProps.add(graph.nodeProperties("w").doubleValue(nodeId)));
        assertThat(nodeProps.size()).isEqualTo(3);
        assertThat(nodeProps).isEqualTo(List.of(1D, 2D, 3D));
    }

    @Test
    void testForAllRelationships() {
        var graph = fromGdl("(a),(b),(c),(a)-[{w:1}]->(b),(a)-[{w:2}]->(c),(b)-[{w:3}]->(c)");
        Set<Double> relProps = Sets.newHashSet();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                relProps.add(w);
                return true;
            });
            return true;
        });
        assertThat(relProps.size()).isEqualTo(3);
        assertThat(relProps).isEqualTo(Set.of(1D, 2D, 3D));
    }
}
