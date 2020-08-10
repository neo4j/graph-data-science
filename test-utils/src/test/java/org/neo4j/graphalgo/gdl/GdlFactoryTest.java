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
package org.neo4j.graphalgo.gdl;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GdlFactoryTest {

    private Graph fromGdl(String gdl) {
        return GdlFactory.of(gdl).build().graphStore().getUnion();
    }

    @Test
    void testInvariants() {
        Graph graph = fromGdl("(a { foo: 42, bar: NaN }), (b { foo: 23, bar: 42.0d }), (a)-[{ weight: 42 }]->(b)");
        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.relationshipCount());
        assertEquals(Sets.newHashSet("foo", "bar"), graph.availableNodeProperties());
        assertTrue(graph.hasRelationshipProperty());
        assertFalse(graph.isUndirected());
        assertFalse(graph.isEmpty());
    }

    @Test
    void testNodeLabels() {
        Graph graph = fromGdl("(a:A),(b:B1:B2),(c)");
        assertEquals(3, graph.nodeCount());
        assertEquals(0, graph.relationshipCount());
        Set<NodeLabel> expectedLabels = Sets.newHashSet("A", "B1", "B2").stream()
            .map(NodeLabel::new)
            .collect(Collectors.toSet());
        assertEquals(expectedLabels, graph.availableNodeLabels());
    }

    @Test
    void testCompatibleListProperties() {
        Graph graph = fromGdl("({f1: [1L, 3L, 3L, 7L], f2: [1.0D, 3.0D, 3.0D, 7.0D]})");
        assertArrayEquals(new long[]{1, 3, 3, 7}, graph.nodeProperties("f1").longArrayValue(0));
        assertArrayEquals(new double[]{1, 3, 3, 7}, graph.nodeProperties("f2").doubleArrayValue(0));
    }

    @Test
    void testIncompatibleListProperties() {
        var ex = assertThrows(IllegalArgumentException.class, () -> fromGdl("({f1: [1, 3, 3, 7]})"));
        assertThat(ex.getMessage(), containsString("Integer"));
        ex = assertThrows(IllegalArgumentException.class, () -> fromGdl("({f1: [1.0, 3.0, 3.0, 7.0]})"));
        assertThat(ex.getMessage(), containsString("Float"));
    }

    @Test
    void testMixedListProperties() {
        var ex = assertThrows(IllegalArgumentException.class, () -> fromGdl("({f1: [4L, 2.0D]})"));
        assertThat(ex.getMessage(), containsString("[Long, Double]"));
    }

    @Test
    void testForAllNodes() {
        Graph graph = fromGdl("({w:1}),({w:2}),({w:3})");
        List<Double> nodeProps = Lists.newArrayList();
        graph.forEachNode(nodeId -> nodeProps.add(graph.nodeProperties("w").doubleValue(nodeId)));
        assertEquals(3, nodeProps.size());
        assertEquals(Arrays.asList(1d, 2d, 3d), nodeProps);
    }

    @Test
    void testForAllRelationships() {
        Graph graph = fromGdl("(a),(b),(c),(a)-[{w:1}]->(b),(a)-[{w:2}]->(c),(b)-[{w:3}]->(c)");
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
}
