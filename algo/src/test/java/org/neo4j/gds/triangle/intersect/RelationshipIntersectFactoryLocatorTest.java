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
package org.neo4j.gds.triangle.intersect;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RelationshipIntersectFactoryLocatorTest {

    @Test
    void supportsHugeGraph() {
        var testGraph = TestSupport.fromGdl("()-->()");
        assertThat(testGraph.graph()).isInstanceOf(HugeGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(testGraph)).isPresent();
    }

    @Test
    void supportsUnionGraph() {
        var testGraph = TestSupport.fromGdl("()-[:A]->()-[:B]->()");
        assertThat(testGraph.graph()).isInstanceOf(UnionGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(testGraph)).isPresent();
    }

    @Test
    void supportsTestGraph() {
        var graphStore = GdlFactory.of("(:A)-[:A]->(:A)-[:B]->(:B)").build().graphStore();
        var testGraph = graphStore.getGraph("A", "A", Optional.empty());
        assertThat(testGraph).isInstanceOf(NodeFilteredGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(testGraph)).isPresent();
    }
}
