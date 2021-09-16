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
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RelationshipIntersectFactoryLocatorTest {

    @Test
    void supportsHugeGraph() {
        var graph = TestSupport.fromGdl("()-->()").graph();
        assertThat(graph).isInstanceOf(HugeGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(graph)).isPresent();
    }

    @Test
    void supportsUnionGraph() {
        var graph = TestSupport.fromGdl("()-[:A]->()-[:B]->()").graph();
        assertThat(graph).isInstanceOf(UnionGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(graph)).isPresent();
    }

    @Test
    void supportsNodeFilteredGraph() {
        var graph = GdlFactory
            .of("(:A)-[:A]->(:A)-[:B]->(:B)")
            .build()
            .graphStore()
            .getGraph("A", "A", Optional.empty());
        assertThat(graph).isInstanceOf(NodeFilteredGraph.class);
        assertThat(RelationshipIntersectFactoryLocator.lookup(graph)).isPresent();
    }
}
