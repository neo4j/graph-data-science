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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.HeapControlTest;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.RelationshipWeightConfigTest;
import org.neo4j.gds.SourceNodeConfigTest;
import org.neo4j.gds.TargetNodeConfigTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.gds.paths.ShortestPathBaseConfig.SOURCE_NODE_KEY;
import static org.neo4j.gds.paths.ShortestPathBaseConfig.TARGET_NODE_KEY;
import static org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig.K_KEY;

abstract class ShortestPathYensProcTest<CONFIG extends ShortestPathYensBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<Yens, CONFIG, DijkstraResult>,
    MemoryEstimateTest<Yens, CONFIG, DijkstraResult>,
    HeapControlTest<Yens, CONFIG, DijkstraResult>,
    RelationshipWeightConfigTest<Yens, CONFIG, DijkstraResult>,
    SourceNodeConfigTest<Yens, CONFIG, DijkstraResult>,
    TargetNodeConfigTest<Yens, CONFIG, DijkstraResult> {

    protected static final String GRAPH_NAME = "graph";
    long idC, idH, idD, idE, idF, idG;
    long[] ids0, ids1, ids2;
    double[] costs0, costs1, costs2;

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           "  (:Offset)" +
           ", (c:Label)" +
           ", (d:Label)" +
           ", (e:Label)" +
           ", (f:Label)" +
           ", (g:Label)" +
           ", (h:Label)" +
           ", (c)-[:TYPE {cost: 3.0}]->(d)" +
           ", (c)-[:TYPE {cost: 2.0}]->(e)" +
           ", (d)-[:TYPE {cost: 4.0}]->(f)" +
           ", (e)-[:TYPE {cost: 1.0}]->(d)" +
           ", (e)-[:TYPE {cost: 2.0}]->(f)" +
           ", (e)-[:TYPE {cost: 3.0}]->(g)" +
           ", (f)-[:TYPE {cost: 2.0}]->(g)" +
           ", (f)-[:TYPE {cost: 1.0}]->(h)" +
           ", (g)-[:TYPE {cost: 2.0}]->(h)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class
        );

        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");
        idG = idFunction.of("g");
        idH = idFunction.of("h");

        ids0 = new long[]{idC, idE, idF, idH};
        ids1 = new long[]{idC, idE, idG, idH};
        ids2 = new long[]{idC, idD, idF, idH};

        costs0 = new double[]{0.0, 2.0, 4.0, 5.0};
        costs1 = new double[]{0.0, 2.0, 5.0, 7.0};
        costs2 = new double[]{0.0, 3.0, 7.0, 8.0};

        runQuery(GdsCypher.call()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .graphCreate(GRAPH_NAME)
            .yields());
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper
            .withNumber(SOURCE_NODE_KEY, idFunction.of("c"))
            .withNumber(TARGET_NODE_KEY, idFunction.of("h"))
            .withNumber(K_KEY, 3);
    }

    @Override
    public void assertResultEquals(DijkstraResult result1, DijkstraResult result2) {
        Assertions.assertEquals(result1.pathSet(), result2.pathSet());
    }

    @Test
    @Disabled
    @Override
    public void testRunOnEmptyGraph() {
        // graph must not be empty
    }

    // disabling tests from org.neo4j.graphalgo.RelationshipWeightConfigTest

    // The following tests are disabled since we have no means of
    // setting a valid source and/or target node id to succeed in
    // graphstore+config validation.

    @Test
    @Disabled
    @Override
    public void testRunUnweightedOnWeightedNoRelTypeGraph() {}

    @Test
    @Disabled
    @Override
    public void testRunUnweightedOnWeightedMultiRelTypeGraph(String relType, String expectedGraph) {}

    @Test
    @Disabled
    @Override
    public void testFilteringOnRelationshipPropertiesOnLoadedGraph(String propertyName, double expectedWeight) {}

    @Test
    @Disabled
    @Override
    public void testFilteringOnRelTypesOnLoadedGraph() {}

    @Test
    @Disabled
    @Override
    public void testRunUnweightedOnWeightedImplicitlyLoadedGraph() {}

    // end of disabled tests from org.neo4j.graphalgo.RelationshipWeightConfigTest
}
