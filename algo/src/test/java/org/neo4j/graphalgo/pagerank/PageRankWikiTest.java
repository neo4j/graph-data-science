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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.HashMap;

import static org.neo4j.graphalgo.pagerank.PageRankTest.assertResult;

@GdlExtension
class PageRankWikiTest {

    @GdlGraph
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            ", (j:Node)" +
            ", (k:Node)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(a)" +
            ", (d)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(b)" +
            ", (f)-[:TYPE]->(e)" +
            ", (g)-[:TYPE]->(b)" +
            ", (g)-[:TYPE]->(e)" +
            ", (h)-[:TYPE]->(b)" +
            ", (h)-[:TYPE]->(e)" +
            ", (i)-[:TYPE]->(b)" +
            ", (i)-[:TYPE]->(e)" +
            ", (j)-[:TYPE]->(e)" +
            ", (k)-[:TYPE]->(e)";

    @Inject
    private TestGraph graph;

    @Test
    void test() {
        var expected = new HashMap<Long, Double>();
        expected.put(graph.toMappedNodeId("a"), 0.3040965);
        expected.put(graph.toMappedNodeId("b"), 3.5658695);
        expected.put(graph.toMappedNodeId("c"), 3.180981);
        expected.put(graph.toMappedNodeId("d"), 0.3625935);
        expected.put(graph.toMappedNodeId("e"), 0.7503465);
        expected.put(graph.toMappedNodeId("f"), 0.3625935);
        expected.put(graph.toMappedNodeId("g"), 0.15);
        expected.put(graph.toMappedNodeId("h"), 0.15);
        expected.put(graph.toMappedNodeId("i"), 0.15);
        expected.put(graph.toMappedNodeId("j"), 0.15);
        expected.put(graph.toMappedNodeId("k"), 0.15);

        assertResult(graph, PageRankAlgorithmType.NON_WEIGHTED, expected);
    }
}
