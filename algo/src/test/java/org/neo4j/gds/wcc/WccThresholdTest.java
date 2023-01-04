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
package org.neo4j.gds.wcc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.ids;
import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

@GdlExtension
class WccThresholdTest {

    private static final String DB =
        "CREATE" +
        "  (a:Label {seedId: 42})" +
        ", (b:Label {seedId: 42})" +
        ", (c:Label {seedId: 42})" +
        ", (d:Label {seedId: 42})" +
        ", (e)" +
        ", (f)" +
        ", (g)" +
        ", (h)" +
        ", (i)" +
        ", (j)" +
        // {A, B, C, D}
        ", (a)-[:TYPE {cost: 10.0}]->(b)" +
        ", (b)-[:TYPE {cost: 10.0}]->(c)" +
        ", (c)-[:TYPE {cost: 10.0}]->(d)" +
        ", (d)-[:TYPE {cost:  4.2}]->(e)" + // threshold UF should split here
        // {E, F, G}
        ", (e)-[:TYPE {cost: 10.0}]->(f)" +
        ", (f)-[:TYPE {cost: 10.0}]->(g)" +
        // {H, I}
        ", (h)-[:TYPE {cost: 10.0}]->(i)";

    @GdlGraph(orientation = NATURAL)
    private static final String DIRECTED_DB = DB;

    @GdlGraph(orientation = UNDIRECTED, graphNamePrefix = "undirected")
    private static final String UNDIRECTED_DB = DB;

    @GdlGraph(orientation = NATURAL, indexInverse = true, graphNamePrefix = "indexed")
    private static final String DIRECTED_INDEXED_DB = DB;

    @Inject
    private TestGraph graph;

    @Inject
    private TestGraph undirectedGraph;

    @Inject
    private TestGraph indexedGraph;

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.wcc.WccThresholdTest#thresholdParams")
    void testDirected(double threshold, String[][] expectedComponents) {
        assertResults(threshold, graph, expectedComponents);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.wcc.WccThresholdTest#thresholdParams")
    void testUndirected(double threshold, String[][] expectedComponents) {
        assertResults(threshold, undirectedGraph, expectedComponents);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.wcc.WccThresholdTest#thresholdParams")
    void testIndexed(double threshold, String[][] expectedComponents) {
        assertResults(threshold, indexedGraph, expectedComponents);
    }

    private void assertResults(double threshold, TestGraph graph, String[][] expectedComponents) {
        WccStreamConfig wccConfig = ImmutableWccStreamConfig
            .builder()
            .threshold(threshold)
            .relationshipWeightProperty("cost")
            .build();

        DisjointSetStruct dss = new Wcc(
            graph,
            Pools.DEFAULT,
            DEFAULT_BATCH_SIZE,
            wccConfig,
            ProgressTracker.NULL_TRACKER
        ).compute();

        long[] communityData = new long[(int) graph.nodeCount()];
        graph.forEachNode(nodeId -> {
            communityData[(int) nodeId] = dss.setIdOf(nodeId);
            return true;
        });

        CommunityHelper.assertCommunities(communityData, ids(graph::toMappedNodeId, expectedComponents));
    }

    static Stream<Arguments> thresholdParams() {
        return Stream.of(
            arguments(
                5.0,
                new String[][]{
                    new String[]{"a", "b", "c", "d"},
                    new String[]{"e", "f", "g"},
                    new String[]{"h", "i"},
                    new String[]{"j"}
                }
            ),
            arguments(
                3.14,
                new String[][]{
                    new String[]{"a", "b", "c", "d", "e", "f", "g"},
                    new String[]{"h", "i"},
                    new String[]{"j"}
                }
            )
        );
    }
}
