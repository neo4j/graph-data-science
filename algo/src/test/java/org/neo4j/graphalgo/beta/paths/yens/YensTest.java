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
package org.neo4j.graphalgo.beta.paths.yens;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.yens.config.ImmutableShortestPathYensStreamConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.beta.paths.PathTestUtil.expected;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
class YensTest {

    static ImmutableShortestPathYensStreamConfig.Builder defaultSourceTargetConfigBuilder() {
        return ImmutableShortestPathYensStreamConfig.builder()
            .path(true)
            .concurrency(1);
    }

    // https://en.wikipedia.org/wiki/Yen%27s_algorithm#/media/File:Yen's_K-Shortest_Path_Algorithm,_K=3,_A_to_F.gif
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (c {id: 0})" +
        ", (d {id: 1})" +
        ", (e {id: 2})" +
        ", (f {id: 3})" +
        ", (g {id: 4})" +
        ", (h {id: 5})" +
        ", (c)-[:REL {cost: 3.0}]->(d)" +
        ", (c)-[:REL {cost: 2.0}]->(e)" +
        ", (d)-[:REL {cost: 4.0}]->(f)" +
        ", (e)-[:REL {cost: 1.0}]->(d)" +
        ", (e)-[:REL {cost: 2.0}]->(f)" +
        ", (e)-[:REL {cost: 3.0}]->(g)" +
        ", (f)-[:REL {cost: 2.0}]->(g)" +
        ", (f)-[:REL {cost: 1.0}]->(h)" +
        ", (g)-[:REL {cost: 2.0}]->(h)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void fixedK() {
        var expected = Set.of(
            expected(graph, idFunction, 0, "c", "e", "f", "h"),
            expected(graph, idFunction, 1, "c", "e", "g", "h"),
            expected(graph, idFunction, 2, "c", "d", "f", "h")
        );

        var config = defaultSourceTargetConfigBuilder()
            .sourceNode(idFunction.of("c"))
            .targetNode(idFunction.of("h"))
            .k(3)
            .build();

        var paths = Yens
            .sourceTarget(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
            .compute()
            .pathSet();

        assertEquals(expected, paths);
    }

    @Test
    void shouldLogProgress() {
        int k = 3;
        var testLogger = new TestProgressLogger(graph.relationshipCount(), "Yens", 1);

        var config = defaultSourceTargetConfigBuilder()
            .sourceNode(idFunction.of("c"))
            .targetNode(idFunction.of("h"))
            .k(k)
            .build();

        var ignored = Yens
            .sourceTarget(graph, config, testLogger, AllocationTracker.empty())
            .compute()
            .pathSet();

        assertEquals(6, testLogger.getProgresses().size());

        // once
        assertTrue(testLogger.containsMessage(TestLog.INFO, "Yens :: Start"));
        assertTrue(testLogger.containsMessage(TestLog.INFO, "Yens :: Finished"));
        // for each k
        for (int i = 1; i <= k; i++) {
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Yens Start searching path %d of %d", i, k)));
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Yens Finish searching path %d of %d", i, k)));

        }
        // multiple times within each k
        assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Yens Start Dijkstra for spur node")));
        assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Dijkstra :: Start")));
        assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Dijkstra :: Finished")));
    }
}
