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
package org.neo4j.graphalgo.beta.filter.graphstore;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.TestSupport.graphStoreFromGDL;
import static org.neo4j.graphalgo.beta.filter.graphstore.GraphStoreFilter.subGraph;

class GraphStoreFilterTest {

    @Test
    void filterNodesOnLabels() throws ParseException {
        var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

        var filteredGraphStore = subGraph(
            graphStore,
            "n:A",
            "true"
        );

        assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"));
        assertGraphEquals(fromGdl("(:A)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMultipleNodesOnLabels() throws ParseException {
        var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

        var filteredGraphStore = subGraph(
            graphStore,
            "n:A OR n:B",
            "true"
        );

        assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(
            NodeLabel.of("A"),
            NodeLabel.of("B")
        );
        assertGraphEquals(fromGdl("(:A), (:B)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterNodeProperties() throws ParseException {
        var graphStore = graphStoreFromGDL(
            "({prop: 42, ignore: 0}), ({prop: 84, ignore: 0}), ({prop: 1337, ignore: 0})");

        var filteredGraphStore = subGraph(
            graphStore,
            "n.prop >= 42 AND n.prop <= 84",
            "true"
        );

        assertGraphEquals(fromGdl("({prop: 42, ignore: 0}), ({prop: 84, ignore: 0})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMultipleNodeProperties() throws ParseException {
        var graphStore = graphStoreFromGDL("({prop1: 42, prop2: 84}), ({prop1: 42, prop2: 42}), ({{prop1: 84, prop2: 84}})");

        var filteredGraphStore = subGraph(
            graphStore,
            "n.prop1 = 42 AND n.prop2 = 84",
            "true"
        );

        assertGraphEquals(fromGdl("({prop1: 42, prop2: 84})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterPropertiesAndLabels() throws ParseException {
        var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B {prop: 84}), (:C)");

        var filteredGraphStore = subGraph(
            graphStore,
            "(n:A AND n.prop = 42) OR (n:B AND n.prop = 84)",
            "true"
        );

        assertGraphEquals(fromGdl("(:A {prop: 42}), (:B {prop: 84})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMissingNodeProperties() throws ParseException {
        var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B)");

        var filteredGraphStore = subGraph(
            graphStore,
            "n.prop = 42",
            "true"
        );

        assertGraphEquals(fromGdl("(:A {prop: 42}))"), filteredGraphStore.getUnion());
    }

    @Test
    void keepAllNodeProperties() throws ParseException {
        var gdl = "(:A {long: 42L, double: 42.0D, longArray: [42L], floatArray: [42.0F], doubleArray: [42.0D]})";
        var graphStore = graphStoreFromGDL(gdl);

        var filteredGraphStore = subGraph(
            graphStore,
            "true",
            "true"
        );

        assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(graphStore.schema().nodeSchema());
        assertGraphEquals(fromGdl(gdl), filteredGraphStore.getUnion());
    }

    @Test
    void removeEmptyNodeSchemaEntries() throws ParseException {
        var graphStore = graphStoreFromGDL("(:A {aProp: 42L}), (:B {bProp: 42L})");

        var filteredGraphStore = subGraph(
            graphStore,
            "n:A",
            "true"
        );

        var aSchema = graphStore
            .schema()
            .nodeSchema()
            .filter(Set.of(NodeLabel.of("A")));

        assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(aSchema);
        assertGraphEquals(fromGdl("(:A {aProp: 42L})"), filteredGraphStore.getUnion());
    }

}
