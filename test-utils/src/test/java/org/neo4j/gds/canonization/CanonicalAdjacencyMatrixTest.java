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
package org.neo4j.gds.canonization;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.canonization.CanonicalAdjacencyMatrix.canonicalize;

class CanonicalAdjacencyMatrixTest {

    @Test
    void testTopologyEquals() {
        Graph g1 = fromGdl("(a), (b), (a)-->(b)");
        Graph g2 = fromGdl("(a), (b), (a)-->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyNotEquals() {
        Graph g1 = fromGdl("(a), (b), (a)-->(b)");
        Graph g2 = fromGdl("(a), (a)-->(a)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyAndNodeLabelsEquals() {
        Graph g1 = fromGdl("(a:A:B), (b:B), (a)-->(b)");
        Graph g2 = fromGdl("(a:A:B), (b:B), (a)-->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyAndNodeLabelsNotEquals() {
        Graph g1 = fromGdl("(a:A:B), (b:B), (a)-->(b)");
        Graph g2 = fromGdl("(a:A:B), (b:C), (a)-->(b)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testTopologyAndDataEquals() {
        Graph g1 = fromGdl("(a {a:2, w:1, q:NaN, f1: [1L, 3L, 3L, 7L], f2: [1.0D, 3.0D, 3.0D, 7.0D], f3: [1.0, 3.0, 3.0, 7.0]}), (b {w:2, a:3, q:42.0d}), (a)-->(b)");
        Graph g2 = fromGdl("(a {a:2, w:1, q:NaN, f1: [1L, 3L, 3L, 7L], f2: [1.0D, 3.0D, 3.0D, 7.0D], f3: [1.0, 3.0, 3.0, 7.0]}), (b {w:2, a:3, q:42.0d}), (a)-->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testParallelEdges() {
        Graph g1 = fromGdl("(a), (b), (a)-[{w:1}]->(b), (a)-[{w:2}]->(b)");
        Graph g2 = fromGdl("(a), (b), (a)-[{w:2}]->(b), (a)-[{w:1}]->(b)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testLoop() {
        Graph g1 = fromGdl("(a), (b), (a)-[{w:1}]->(a), (a)-[{w:2}]->(b)");
        Graph g2 = fromGdl("(a), (b), (a)-[{w:2}]->(b), (a)-[{w:1}]->(a)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCycle() {
        Graph g1 = fromGdl("(a {v:1}), (b {v:2}), (c {v:3}), (a)-->(b)-->(c)-->(a)");
        Graph g2 = fromGdl("(a {v:2}), (b {v:3}), (c {v:1}), (a)-->(b)-->(c)-->(a)");
        assertEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCompleteGraph() {
        Graph g1 = fromGdl(
            "(a {v:1}), (b {v:2}), (c {v:3}), (b)<--(a)-->(c), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        Graph g2 = fromGdl(
            "(a {v:1}), (b {v:2}), (c {v:3}), (b)<--(a)-->(b), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testCompleteHomogenicGraph() {
        Graph g1 = fromGdl(
            "(a {v:1}), (b {v:1}), (c {v:1}), (b)<--(a)-->(c), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        Graph g2 = fromGdl(
            "(a {v:1}), (b {v:1}), (c {v:1}), (b)<--(a)-->(b), (a)<--(b)-->(c), (a)<--(c)-->(b)");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }

    @Test
    void testRespectNodeSchema() {
        Graph g1 = fromGdl("(a:A {aV:1.0}), (b:B {bV:2.0}), (c:V {cV:3.0})");
        Graph g2 = fromGdl("(a:A {aV:1.0, bV:NaN, cV:NaN}), (b:B {aV:NaN, bV:2.0, cV:NaN}), (c:V {aV:NaN, bV:NaN, cV:3.0})");
        assertNotEquals(canonicalize(g1), canonicalize(g2));
    }
}
