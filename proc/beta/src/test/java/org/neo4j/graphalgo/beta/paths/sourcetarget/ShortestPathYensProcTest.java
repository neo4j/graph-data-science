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
package org.neo4j.graphalgo.beta.paths.sourcetarget;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.RelationshipWeightConfigTest;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.yens.Yens;
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.nodeIdByProperty;
import static org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig.SOURCE_NODE_KEY;
import static org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig.TARGET_NODE_KEY;
import static org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig.K_KEY;

abstract class ShortestPathYensProcTest<CONFIG extends ShortestPathYensBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<Yens, CONFIG, DijkstraResult>,
    MemoryEstimateTest<Yens, CONFIG, DijkstraResult>,
    HeapControlTest<Yens, CONFIG, DijkstraResult>,
    RelationshipWeightConfigTest<Yens, CONFIG, DijkstraResult> {

    long idC, idH, idD, idE, idF, idG;
    long[] ids0, ids1, ids2;
    double[] costs0, costs1, costs2;

    @Override
    public String createQuery() {
        return "CREATE" +
               "  (c {id: 1})" +
               ", (d {id: 2})" +
               ", (e {id: 3})" +
               ", (f {id: 4})" +
               ", (g {id: 5})" +
               ", (h {id: 6})" +
               ", (c)-[:TYPE {cost: 3.0}]->(d)" +
               ", (c)-[:TYPE {cost: 2.0}]->(e)" +
               ", (d)-[:TYPE {cost: 4.0}]->(f)" +
               ", (e)-[:TYPE {cost: 1.0}]->(d)" +
               ", (e)-[:TYPE {cost: 2.0}]->(f)" +
               ", (e)-[:TYPE {cost: 3.0}]->(g)" +
               ", (f)-[:TYPE {cost: 2.0}]->(g)" +
               ", (f)-[:TYPE {cost: 1.0}]->(h)" +
               ", (g)-[:TYPE {cost: 2.0}]->(h)";
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class
        );
        runQuery(createQuery());

        idC = nodeIdByProperty(db, 1);
        idD = nodeIdByProperty(db, 2);
        idE = nodeIdByProperty(db, 3);
        idF = nodeIdByProperty(db, 4);
        idG = nodeIdByProperty(db, 5);
        idH = nodeIdByProperty(db, 6);

        ids0 = new long[]{idC, idE, idF, idH};
        ids1 = new long[]{idC, idE, idG, idH};
        ids2 = new long[]{idC, idD, idF, idH};

        costs0 = new double[]{0.0, 2.0, 4.0, 5.0};
        costs1 = new double[]{0.0, 2.0, 5.0, 7.0};
        costs2 = new double[]{0.0, 3.0, 7.0, 8.0};

        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .graphCreate("graph")
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
            .withNumber(SOURCE_NODE_KEY, nodeIdByProperty(db, 1))
            .withNumber(TARGET_NODE_KEY, nodeIdByProperty(db, 6))
            .withNumber(K_KEY, 3);
    }

    @Override
    public void assertResultEquals(DijkstraResult result1, DijkstraResult result2) {
        assertEquals(result1.pathSet(), result2.pathSet());
    }
}
