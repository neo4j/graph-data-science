/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.canonization;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestGraph;
import org.neo4j.graphalgo.api.Graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphalgo.canonization.CanonicalAdjacencyMatrix.canonicalize;

class CanonicalAdjacencyMatrixTest {

    @Test
    void testTopologyEquals() {
        Graph g1 = TestGraph.Builder.fromGdl("(a), (b), (a)-->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a), (b), (a)-->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyNotEquals() {
        Graph g1 = TestGraph.Builder.fromGdl("(a), (b), (a)-->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a), (a)-->(a)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyAndDataEquals() {
        Graph g1 = TestGraph.Builder.fromGdl("(a {a:2, w:1}), (b {w:2, a:3}), (a)-->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a {a:2, w:1}), (b {w:2, a:3}), (a)-->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testParallelEdges() {
        Graph g1 = TestGraph.Builder.fromGdl("(a), (b), (a)-[{w:1}]->(b), (a)-[{w:2}]->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a), (b), (a)-[{w:2}]->(b), (a)-[{w:1}]->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testLoop() {
        Graph g1 = TestGraph.Builder.fromGdl("(a), (b), (a)-[{w:1}]->(a), (a)-[{w:2}]->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a), (b), (a)-[{w:2}]->(b), (a)-[{w:1}]->(a)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCycle() {
        Graph g1 = TestGraph.Builder.fromGdl("(a {v:1}), (b {v:2}), (c {v:3}), (a)-->(b)-->(c)-->(a)");
        Graph g2 = TestGraph.Builder.fromGdl("(a {v:2}), (b {v:3}), (c {v:1}), (a)-->(b)-->(c)-->(a)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCompleteGraph() {
        Graph g1 = TestGraph.Builder.fromGdl("(a {v:1}), (b {v:2}), (c {v:3}), (b)<--(a)-->(c), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a {v:1}), (b {v:2}), (c {v:3}), (b)<--(a)-->(b), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCompleteHomogenicGraph() {
        Graph g1 = TestGraph.Builder.fromGdl("(a {v:1}), (b {v:1}), (c {v:1}), (b)<--(a)-->(c), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        Graph g2 = TestGraph.Builder.fromGdl("(a {v:1}), (b {v:1}), (c {v:1}), (b)<--(a)-->(b), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }
}
