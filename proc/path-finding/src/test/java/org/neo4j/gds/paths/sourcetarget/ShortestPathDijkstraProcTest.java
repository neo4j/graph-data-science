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
package org.neo4j.gds.paths.sourcetarget;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.SourceNodeConfigTest;
import org.neo4j.gds.TargetNodeConfigTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.ShortestPathBaseConfig;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.paths.ShortestPathBaseConfig.SOURCE_NODE_KEY;
import static org.neo4j.gds.paths.ShortestPathBaseConfig.TARGET_NODE_KEY;

abstract class ShortestPathDijkstraProcTest<CONFIG extends ShortestPathBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<Dijkstra, CONFIG, DijkstraResult>,
    MemoryEstimateTest<Dijkstra, CONFIG, DijkstraResult>,
    SourceNodeConfigTest<Dijkstra, CONFIG, DijkstraResult>,
    TargetNodeConfigTest<Dijkstra, CONFIG, DijkstraResult> {

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return modeSpecificConfigTests();
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    protected static final String GRAPH_NAME = "graph";
    long idA, idC, idD, idE, idF;
    static long[] ids0;
    static double[] costs0;

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE" +
           "  (:Offset)" +
           ", (a:Label)" +
           ", (b:Label)" +
           ", (c:Label)" +
           ", (d:Label)" +
           ", (e:Label)" +
           ", (f:Label)" +
           ", (a)-[:TYPE {cost: 4}]->(b)" +
           ", (a)-[:TYPE {cost: 2}]->(c)" +
           ", (b)-[:TYPE {cost: 5}]->(c)" +
           ", (b)-[:TYPE {cost: 10}]->(d)" +
           ", (c)-[:TYPE {cost: 3}]->(e)" +
           ", (d)-[:TYPE {cost: 11}]->(f)" +
           ", (e)-[:TYPE {cost: 4}]->(d)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );

        idA = idFunction.of("a");
        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");

        ids0 = new long[]{idA, idC, idE, idD, idF};
        costs0 = new double[]{0.0, 2.0, 5.0, 9.0, 20.0};

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper
            .withNumber(SOURCE_NODE_KEY, idFunction.of("a"))
            .withNumber(TARGET_NODE_KEY, idFunction.of("f"));
    }

    @Override
    public void assertResultEquals(DijkstraResult result1, DijkstraResult result2) {
        assertEquals(result1.pathSet(), result2.pathSet());
    }

    @Test
    @Disabled
    @Override
    public void testRunOnEmptyGraph() {
        // graph must not be empty
    }
}
