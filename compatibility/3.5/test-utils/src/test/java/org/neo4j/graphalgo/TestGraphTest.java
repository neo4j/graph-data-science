/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Element;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestGraphTest {

    @Test
    void testConsecutiveIdSpaceForGDLGraph() {
        GDLHandler gdlHandler = new GDLHandler.Builder()
                .buildFromString("(a)-->(b),(b)-->(c),(c)-->(a)," +
                                 "(),(),()" +
                                 "()-->(),()-->()");

        assertTrue(haveConsecutiveIdSpace(gdlHandler.getVertices()));
        assertTrue(haveConsecutiveIdSpace(gdlHandler.getEdges()));
    }

    private static boolean haveConsecutiveIdSpace(Collection<? extends Element> elements) {
        long maxVertexId = elements.parallelStream().mapToLong(Element::getId).max().orElse(-1);
        return (maxVertexId == elements.size() - 1);
    }

    @Test
    void testInvariants() {
        Graph graph = TestGraph.Builder.fromGdl("(a { foo: 42 }), (b { foo: 23 }), (a)-[{ weight: 42 }]->(b)");
        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.relationshipCount());
        assertEquals(Sets.newHashSet("foo"), graph.availableNodeProperties());
        assertTrue(graph.hasRelationshipProperty());
        assertFalse(graph.isUndirected());
        assertFalse(graph.isEmpty());
        assertEquals(TestGraph.TYPE, graph.getType());
    }

    @Test
    void testForAllNodes() {
        Graph graph = TestGraph.Builder.fromGdl("({w:1}),({w:2}),({w:3})");
        List<Double> nodeProps = Lists.newArrayList();
        graph.forEachNode(nodeId -> nodeProps.add(graph.nodeProperties("w").nodeProperty(nodeId)));
        assertEquals(3, nodeProps.size());
        assertEquals(Arrays.asList(1d, 2d, 3d), nodeProps);
    }

    @Test
    void testForAllRelationships() {
        Graph graph = TestGraph.Builder.fromGdl("(a),(b),(c),(a)-[{w:1}]->(b),(a)-[{w:2}]->(c),(b)-[{w:3}]->(c)");
        Set<Double> relProps = Sets.newHashSet();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                relProps.add(w);
                return true;
            });
            return true;
        });
        assertEquals(3, relProps.size());
        assertEquals(Sets.newHashSet(1d, 2d, 3d), relProps);
    }

    @Test
    void invalidNodeProperties() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> TestGraph.Builder.fromGdl("(a { foo: 42 }), (b { bar: 23 })"));
        assertThat(ex.getMessage(), containsString("Vertices must have the same set of property keys."));
    }

    @Test
    void invalidRelationshipProperties() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> TestGraph.Builder.fromGdl("(a { foo: 42 }), (b { foo: 23 }), (a)-[{w:1}]->(b), (a)-[{q:1}]->(b)"));
        assertThat(ex.getMessage(), containsString("Relationships must have the same set of property keys."));
    }

}
