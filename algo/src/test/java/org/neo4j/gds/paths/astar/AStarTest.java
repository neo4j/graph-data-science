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
package org.neo4j.gds.paths.astar;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.paths.astar.config.ImmutableShortestPathAStarStreamConfig;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.paths.PathTestUtil.expected;

@GdlExtension
class AStarTest {

    static ImmutableShortestPathAStarStreamConfig.Builder defaultSourceTargetConfigBuilder() {
        return ImmutableShortestPathAStarStreamConfig.builder()
            .latitudeProperty("latitude")
            .longitudeProperty("longitude")
            .concurrency(1);
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1_000, 56_856L),
            Arguments.of(1_000_000, 56_125_728L),
            Arguments.of(1_000_000_000, 56_133_545_824L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int nodeCount, long expectedBytes) {
        TestSupport.assertMemoryEstimation(
            AStar::memoryEstimation,
            nodeCount,
            1,
            MemoryRange.of(expectedBytes)
        );
    }

    /* Singapore to Chiba
     * Path nA (0NM) -> nB (29NM) -> nC (723NM) -> nD (895NM) -> nE (996NM) -> nF (1353NM)
     * 	    nG (1652NM) -> nH (2392NM) -> nX (2979NM)
     * Distance = 2979 NM
     * */
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (nA:Node {latitude: 1.304444,    longitude: 103.717373})" + // name: 'SINGAPORE'
        ", (nB:Node {latitude: 1.1892,      longitude: 103.4689})" + // name: 'SINGAPORE STRAIT'
        ", (nC:Node {latitude: 8.83055556,  longitude: 111.8725})" + // name: 'WAYPOINT 68'
        ", (nD:Node {latitude: 10.82916667, longitude: 113.9722222})" + // name: 'WAYPOINT 70'
        ", (nE:Node {latitude: 11.9675,     longitude: 115.2366667})" + // name: 'WAYPOINT 74'
        ", (nF:Node {latitude: 16.0728,     longitude: 119.6128})" + // name: 'SOUTH CHINA SEA'
        ", (nG:Node {latitude: 20.5325,     longitude: 121.845})" + // name: 'LUZON STRAIT'
        ", (nH:Node {latitude: 29.32611111, longitude: 131.2988889})" + // name: 'WAYPOINT 87'
        ", (nI:Node {latitude: -2.0428,     longitude: 108.6225})" + // name: 'KARIMATA STRAIT'
        ", (nJ:Node {latitude: -8.3256,     longitude: 115.8872})" + // name: 'LOMBOK STRAIT'
        ", (nK:Node {latitude: -8.5945,     longitude: 116.6867})" + // name: 'SUMBAWA STRAIT'
        ", (nL:Node {latitude: -8.2211,     longitude: 125.2411})" + // name: 'KOLANA AREA'
        ", (nM:Node {latitude: -1.8558,     longitude: 126.5572})" + // name: 'EAST MANGOLE'
        ", (nN:Node {latitude: 3.96861111,  longitude: 128.3052778})" + // name: 'WAYPOINT 88'
        ", (nO:Node {latitude: 12.76305556, longitude: 131.2980556})" + // name: 'WAYPOINT 89'
        ", (nP:Node {latitude: 22.32027778, longitude: 134.700000})" + // name: 'WAYPOINT 90'
        ", (nX:Node {latitude: 35.562222,   longitude: 140.059187})" + // name: 'CHIBA'
        ", (nA)-[:TYPE {cost: 29.0}]->(nB)" +
        ", (nB)-[:TYPE {cost: 694.0}]->(nC)" +
        ", (nC)-[:TYPE {cost: 172.0}]->(nD)" +
        ", (nD)-[:TYPE {cost: 101.0}]->(nE)" +
        ", (nE)-[:TYPE {cost: 357.0}]->(nF)" +
        ", (nF)-[:TYPE {cost: 299.0}]->(nG)" +
        ", (nG)-[:TYPE {cost: 740.0}]->(nH)" +
        ", (nH)-[:TYPE {cost: 587.0}]->(nX)" +
        ", (nB)-[:TYPE {cost: 389.0}]->(nI)" +
        ", (nI)-[:TYPE {cost: 584.0}]->(nJ)" +
        ", (nJ)-[:TYPE {cost: 82.0}]->(nK)" +
        ", (nK)-[:TYPE {cost: 528.0}]->(nL)" +
        ", (nL)-[:TYPE {cost: 391.0}]->(nM)" +
        ", (nM)-[:TYPE {cost: 364.0}]->(nN)" +
        ", (nN)-[:TYPE {cost: 554.0}]->(nO)" +
        ", (nO)-[:TYPE {cost: 603.0}]->(nP)" +
        ", (nP)-[:TYPE {cost: 847.0}]->(nX)";

    @Inject
    Graph graph;

    @Inject
    IdFunction idFunction;

    @Test
    void sourceTarget() {
        var expected = expected(
            idFunction,
            0,
            new double[]{0.0, 29.0, 723.0, 895.0, 996.0, 1353.0, 1652.0, 2392.0, 2979.0},
            "nA", "nB", "nC", "nD", "nE", "nF", "nG", "nH", "nX"
        );

        var config = defaultSourceTargetConfigBuilder()
            .sourceNode(idFunction.of("nA"))
            .targetNode(idFunction.of("nX"))
            .build();

        var path = AStar
            .sourceTarget(graph, config, ProgressTracker.NULL_TRACKER)
            .compute()
            .findFirst()
            .get();

        assertEquals(expected, path);
    }

    @Test
    void shouldLogProgress() {

        var config = defaultSourceTargetConfigBuilder()
            .sourceNode(idFunction.of("nA"))
            .targetNode(idFunction.of("nX"))
            .build();

        var progressTask = new AStarFactory<>().progressTask(graph, config);
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, log, 1, EmptyTaskRegistryFactory.INSTANCE);

        AStar.sourceTarget(graph, config, progressTracker)
            .compute()
            .pathSet();

        List<AtomicLong> progresses = progressTracker.getProgresses();
        assertEquals(1, progresses.size());
        assertEquals(9, progresses.get(0).get());

        assertTrue(log.containsMessage(TestLog.INFO, "AStar :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 5%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 17%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 23%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 29%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 35%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 41%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 47%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar 52%"));
        assertTrue(log.containsMessage(TestLog.INFO, "AStar :: Finished"));

        // no duplicate entries in progress logger
        var logMessages = log.getMessages(TestLog.INFO);
        assertEquals(Set.copyOf(logMessages).size(), logMessages.size());
    }

    // Validated against https://www.vcalc.com/wiki/vCalc/Haversine+-+Distance
    @ParameterizedTest
    @CsvSource(value = {
        "42,1337,1337,42,7264.92",
        "42.42,19.84,13.37,20.77,1744.84"
    })
    void haversineTest(
        double sourceLatitude,
        double sourceLongitude,
        double targetLatitude,
        double targetLongitude,
        double expectedDistance
    ) {
        assertEquals(
            expectedDistance,
            AStar.HaversineHeuristic.distance(sourceLatitude, sourceLongitude, targetLatitude, targetLongitude),
            1E-2
        );
    }
}
