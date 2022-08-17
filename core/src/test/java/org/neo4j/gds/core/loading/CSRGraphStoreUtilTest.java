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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.huge.HugeGraph;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

class CSRGraphStoreUtilTest {

    @Test
    void fromGraph() {
        String GDL =
            "  CREATE" +
            "  (a:A { foo: 42, bar: 1337 })" +
            ", (b:A { foo: 84, bar: 1234 })" +
            ", (c:B { foo: 23 })" +
            // Add one relationship type with a single property
            // to ensure that the graph is a HugeGraph.
            ", (a)-[:REL1 { prop1: 42 }]->(b)";

        var graphStore = TestSupport.graphStoreFromGDL(GDL);
        var graph = graphStore.getUnion();
        assertThat(graph).isInstanceOf(HugeGraph.class);
        assertThat(graph.availableNodeLabels()).contains(NodeLabel.of("A"), NodeLabel.of("B"));
        assertThat(graph.availableNodeProperties()).contains("foo", "bar");
        assertThat(graph.schema().relationshipSchema().availableTypes()).contains(RelationshipType.of("REL1"));
        assertThat(graph.hasRelationshipProperty()).isTrue();

        var convertedGraphStore = CSRGraphStoreUtil.createFromGraph(
            DatabaseId.from("dummy"),
            (HugeGraph) graph,
            "REL1",
            Optional.of("prop1"),
            1
        );

        assertThat(convertedGraphStore.schema()).isEqualTo(graphStore.schema());
        assertGraphEquals(graphStore.getUnion(), convertedGraphStore.getUnion());
    }

    @Test
    void shouldValidateRelationshipPropertyKey() {
        var graph = TestSupport.fromGdl("()-[:REL]->()");

        assertThatThrownBy(() -> {
            CSRGraphStoreUtil.createFromGraph(
                DatabaseId.from("dummy"),
                (HugeGraph) graph.innerGraph(),
                "REL",
                Optional.of("prop1"),
                1
            );
        })
            .hasMessage("Expected relationship property 'prop1', but graph has none.");
    }

}
