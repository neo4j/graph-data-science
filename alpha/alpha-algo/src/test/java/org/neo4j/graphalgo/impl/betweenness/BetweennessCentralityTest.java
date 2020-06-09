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
package org.neo4j.graphalgo.impl.betweenness;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class BetweennessCentralityTest {

    private static final AllocationTracker TRACKER = AllocationTracker.EMPTY;

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction nodeId;

    private static final double[] EXACT_CENTRALITIES = {0.0, 3.0, 4.0, 3.0, 0.0};
    private static final double[] EMPTY_CENTRALITIES = {0.0, 0.0, 0.0, 0.0, 0.0};

    @Test
    void testMultiSourceBC() {
        var bc = new MSBetweennessCentrality(graph, false, 5, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        assertResult(bc.compute(), EXACT_CENTRALITIES);
    }

    @Test
    void testRABrandesForceCompleteSampling() {
        var bc = new RABrandesBetweennessCentrality(graph, new RandomSelectionStrategy(graph, 1.0, TRACKER), Pools.DEFAULT, 1, TRACKER);
        assertResult(bc.compute().getCentrality(), EXACT_CENTRALITIES);
    }

    @Test
    void testRABrandesForceEmptySampling() {
        var bc = new RABrandesBetweennessCentrality(graph, new RandomSelectionStrategy(graph, 0.0, TRACKER), Pools.DEFAULT, 3, TRACKER);
        assertResult(bc.compute().getCentrality(), EMPTY_CENTRALITIES);
    }

    @Disabled
    void testRABrandes() {
        var bc = new RABrandesBetweennessCentrality(graph, new RandomSelectionStrategy(graph, 0.3, TRACKER), Pools.DEFAULT, 3, TRACKER
        );
        assertResult(bc.compute().getCentrality(), EXACT_CENTRALITIES);
    }

    private void assertResult(AtomicDoubleArray result, double[] centralities) {
        assertEquals(5, centralities.length, "Expected 5 centrality values");
        assertEquals(centralities[0], result.get((int) nodeId.of("a")));
        assertEquals(centralities[1], result.get((int) nodeId.of("b")));
        assertEquals(centralities[2], result.get((int) nodeId.of("c")));
        assertEquals(centralities[3], result.get((int) nodeId.of("d")));
        assertEquals(centralities[4], result.get((int) nodeId.of("e")));
    }

    private void assertResult(HugeAtomicDoubleArray result, double[] centralities) {
        assertEquals(5, centralities.length, "Expected 5 centrality values");
        assertEquals(centralities[0], result.get((int) nodeId.of("a")));
        assertEquals(centralities[1], result.get((int) nodeId.of("b")));
        assertEquals(centralities[2], result.get((int) nodeId.of("c")));
        assertEquals(centralities[3], result.get((int) nodeId.of("d")));
        assertEquals(centralities[4], result.get((int) nodeId.of("e")));
    }
}
