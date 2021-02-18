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
package org.neo4j.graphalgo.extension;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.MultiCSRGraph;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class TestGraphTest {

    @Test
    void exposeInner() {
        assertTrue(fromGdl("()").innerGraph() instanceof HugeGraph);

        TestGraph g = new TestGraph(
            GdlFactory
                .of("(:A)-[:T1]->(:B)-[:T2]->(:A)")
                .build()
                .graphStore()
                .getGraph(NodeLabel.listOf("A", "B"), RelationshipType.listOf("T1", "T2"), Optional.empty()),
            (a) -> 0,
            "foo"
        );
        assertTrue(g.innerGraph() instanceof UnionGraph);
    }

    @Test
    void shouldCopyForConcurrentAccess() {
        TestGraph g = fromGdl("()");
        assertNotEquals(g.concurrentCopy(), g);
    }

    @Test
    void shouldHaveAName() {
        TestGraph g = fromGdl("()", "GG");
        assertEquals("GG", g.toString());
    }

    @Test
    void usesIdFunctionForOriginalId() {
        MultiCSRGraph bGraph = GdlFactory
            .of("(:A), (b:B), (:B)")
            .build()
            .graphStore()
            .getGraph(List.of(NodeLabel.of("B")), List.of(RelationshipType.ALL_RELATIONSHIPS), Optional.empty());
        TestGraph g = new TestGraph(bGraph, (a) -> a.equals("b") ? 1 : 2, "foo");
        assertEquals(1, g.toOriginalNodeId("b"));
        assertEquals(2, g.toOriginalNodeId("notB"));
    }

    @Test
    void usesInnerGraphForMappedId() {
        MultiCSRGraph bGraph = GdlFactory
            .of("(:A), (b:B), (:B)")
            .build()
            .graphStore()
            .getGraph(List.of(NodeLabel.of("B")), List.of(RelationshipType.ALL_RELATIONSHIPS), Optional.empty());
        TestGraph g = new TestGraph(bGraph, (a) -> a.equals("b") ? 1 : 2, "foo");
        assertEquals(0, g.toMappedNodeId("b"));
        assertEquals(1, g.toMappedNodeId("notB"));
    }

}
