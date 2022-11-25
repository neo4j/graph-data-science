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
package org.neo4j.gds.gdl;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;

class GdlFactoryTest {

    private Graph fromGdl(String gdl) {
        return GdlFactory.of(gdl).build().getUnion();
    }

    @Test
    void testInvariants() {
        var graph = fromGdl("(a { foo: 42, bar: NaN }), (b { foo: 23, bar: 42.0d }), (a)-[{ weight: 42 }]->(b)");
        assertThat(graph.nodeCount()).isEqualTo(2);
        assertThat(graph.relationshipCount()).isEqualTo(1);
        assertThat(graph.availableNodeProperties()).isEqualTo(Set.of("foo", "bar"));
        assertThat(graph.hasRelationshipProperty()).isTrue();
        assertThat(graph.schema().isUndirected()).isFalse();
        assertThat(graph.isEmpty()).isFalse();
    }

    @Test
    void testNodeLabels() {
        Graph graph = fromGdl("(a:A),(b:B1:B2),(c)");
        assertThat(graph.nodeCount()).isEqualTo(3);
        assertThat(graph.relationshipCount()).isEqualTo(0);
        var expectedLabels = Stream.of("A", "B1", "B2", "__ALL__")
            .map(NodeLabel::new)
            .collect(Collectors.toSet());
        assertThat(graph.availableNodeLabels()).isEqualTo(expectedLabels);
    }

    @Test
    void testRelationshipTypes() {
        var gdlFactory = GdlFactory.of("(a)-[:REL1]->(b)-[:REL2]->(c)");
        var graphStore = gdlFactory.build();
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
        var graphStore = gdlFactory.build();
        var sourceNodeId = gdlFactory.nodeId("a");

        assertRelationshipProperty(graphStore, "REL", "foo", sourceNodeId, 42.0);
        assertRelationshipProperty(graphStore, "REL", "bar", sourceNodeId, 1337.0);
        assertRelationshipProperty(graphStore, "REL", "baz", sourceNodeId, 84.0);
    }

    @Test
    void testRelationshipPropertiesDifferentProperties() {
        var gdlFactory = GdlFactory.of((
            "(a)-[:REL { foo: 42, bar: 1337, baz: 84 }]->(b)" +
            "(b)-[:REL { foo: 42 }]->(c)")
        );
        var graphStore = gdlFactory.build();
        var sourceNodeId = gdlFactory.nodeId("b");

        assertRelationshipProperty(graphStore, "REL", "foo", sourceNodeId, 42.0);
        assertRelationshipProperty(graphStore, "REL", "bar", sourceNodeId, DefaultValue.forDouble().doubleValue());
        assertRelationshipProperty(graphStore, "REL", "baz", sourceNodeId, DefaultValue.forDouble().doubleValue());
    }

    @Test
    void testRelationshipPropertiesMixedSchema() {
        var gdlFactory = GdlFactory.of((
            "(a)-[:REL1 { foo: 42, bar: 1337, baz: 84 }]->(b)" +
            "(b)-[:REL2 { foo: 4.2D, bob: 13.37D }]->(c)")
        );
        var graphStore = gdlFactory.build();
        var idA = gdlFactory.nodeId("a");
        var idB = gdlFactory.nodeId("b");

        assertRelationshipProperty(graphStore, "REL1", "foo", idA, 42.0);
        assertRelationshipProperty(graphStore, "REL2", "foo", idB, 4.2);
        assertRelationshipProperty(graphStore, "REL1", "baz", idA, 84.0);
        assertRelationshipProperty(graphStore, "REL2", "bob", idB, 13.37);
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
        List<Double> nodeProps = new ArrayList<>();
        graph.forEachNode(nodeId -> nodeProps.add(graph.nodeProperties("w").doubleValue(nodeId)));
        assertThat(nodeProps.size()).isEqualTo(3);
        assertThat(nodeProps).isEqualTo(List.of(1D, 2D, 3D));
    }

    @Test
    void testForAllRelationships() {
        var graph = fromGdl("(a),(b),(c),(a)-[{w:1}]->(b),(a)-[{w:2}]->(c),(b)-[{w:3}]->(c)");
        Set<Double> relProps = new HashSet<>();
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

    @Test
    void correctSchema() {
        var graph = fromGdl(
                            "  (a1:A   {double: 42.0D, long: 42L, doubleArray: [42.0D], longArray: [42L]})" +
                            ", (a2b:A  {double: 84.0D})" +
                            ", (ab:A:B {double: 84.0D, long: 1337L})" +
                            ", (c:C)" +
                            ", (a1)-[:A {prop1: 42.0D, prop2: 42.0D}]->(c)" +
                            ", (a1)-[:A {prop1: 84.0D}]->(c)" +
                            ", (a1)-[:B {prop1: 84.0D, prop3: 1337.0D}]->(c)" +
                            ", (a1)-[:C]->(c)"
        );

        var nodeSchema = graph.schema().nodeSchema();
        var expectedNodeSchema = NodeSchema.empty();

        expectedNodeSchema.getOrCreateLabel(NodeLabel.of("A"))
            .addProperty("double", ValueType.DOUBLE)
            .addProperty("long", ValueType.LONG)
            .addProperty("doubleArray", ValueType.DOUBLE_ARRAY)
            .addProperty("longArray", ValueType.LONG_ARRAY);

        expectedNodeSchema.getOrCreateLabel(NodeLabel.of("B"))
            .addProperty("double", ValueType.DOUBLE)
            .addProperty("long", ValueType.LONG);

        expectedNodeSchema.getOrCreateLabel(NodeLabel.of("C"));
        assertThat(nodeSchema).isEqualTo(expectedNodeSchema);

        var relationshipSchema = graph.schema().relationshipSchema();
        var expectedRelationshipSchema = RelationshipSchema.empty();

        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("A"), Direction.DIRECTED)
            .addProperty("prop1", ValueType.DOUBLE)
            .addProperty("prop2", ValueType.DOUBLE);

        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("B"), Direction.DIRECTED)
            .addProperty("prop1", ValueType.DOUBLE)
            .addProperty("prop3", ValueType.DOUBLE);

        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("C"), Direction.DIRECTED);
        assertThat(relationshipSchema).isEqualTo(expectedRelationshipSchema);
    }

    static Stream<Orientation> orientations() {
        return Stream.of(UNDIRECTED, NATURAL);
    }

    @ParameterizedTest
    @MethodSource("orientations")
    void correctRelationshipSchemaDirection(Orientation orientation) {
        var graphStore = GdlFactory.builder().graphProjectConfig(
            ImmutableGraphProjectFromGdlConfig.builder()
                .gdlGraph(
                    "  (a1:A   {double: 42.0D, long: 42L, doubleArray: [42.0D], longArray: [42L]})" +
                    ", (a2b:A  {double: 84.0D})" +
                    ", (ab:A:B {double: 84.0D, long: 1337L})" +
                    ", (c:C)" +
                    ", (a1)-[:A {prop1: 42.0D, prop2: 42.0D}]->(c)" +
                    ", (a1)-[:A {prop1: 84.0D}]->(c)" +
                    ", (a1)-[:B {prop1: 84.0D, prop3: 1337.0D}]->(c)" +
                    ", (a1)-[:C]->(c)"
                ).graphName("test")
                .orientation(orientation)
                .build()
        ).build().build();

        var expectedRelationshipSchema = RelationshipSchema.empty();

        var direction = Direction.fromOrientation(orientation);
        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("A"), direction)
            .addProperty("prop1", ValueType.DOUBLE)
            .addProperty("prop2", ValueType.DOUBLE);

        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("B"), direction)
            .addProperty("prop1", ValueType.DOUBLE)
            .addProperty("prop3", ValueType.DOUBLE);

        expectedRelationshipSchema.getOrCreateRelationshipType(RelationshipType.of("C"), direction);

        assertThat(graphStore.schema().relationshipSchema()).isEqualTo(expectedRelationshipSchema);
    }

    @Test
    void testCustomNodeIdFunction() {
        var nextId = new MutableLong(42);
        var factory = GdlFactory.builder()
            .gdlGraph("(a),(b),(c)")
            .nodeIdFunction(nextId::getAndIncrement)
            .build();

        assertThat(factory.nodeId("a")).isEqualTo(42L);
        assertThat(factory.nodeId("b")).isEqualTo(43L);
        assertThat(factory.nodeId("c")).isEqualTo(44L);

        var importResult = factory.build();

        assertThat(factory.dimensions().highestPossibleNodeCount()).isEqualTo(45L);
        assertThat(importResult.nodes().highestOriginalId()).isEqualTo(44L);
    }

    private void assertRelationshipProperty(
        GraphStore graphStore,
        String relType,
        String propertyKey,
        long sourceNodeId,
        double expected
    ) {
        graphStore.getGraph(RelationshipType.of(relType), Optional.of(propertyKey))
            .forEachRelationship(sourceNodeId, Double.POSITIVE_INFINITY, (ignored, targetNodeId, property) -> {
                if (Double.isNaN(expected)) {
                    assertThat(property).isNaN();
                } else {
                    assertThat(property).isEqualTo(expected);
                }
                return true;
            });
    }
}
