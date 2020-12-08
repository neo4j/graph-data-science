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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
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
import static org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig.SOURCE_NODE_KEY;
import static org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig.TARGET_NODE_KEY;
import static org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig.K_KEY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

abstract class ShortestPathYensProcTest<CONFIG extends ShortestPathYensBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<Yens, CONFIG, DijkstraResult>,
    MemoryEstimateTest<Yens, CONFIG, DijkstraResult>,
    HeapControlTest<Yens, CONFIG, DijkstraResult>,
    RelationshipWeightConfigTest<Yens, CONFIG, DijkstraResult>
{

    @Override
    public String createQuery() {
        return "CREATE" +
               "  (c {id: 1})" +
               ", (d {id: 2})" +
               ", (e {id: 3})" +
               ", (f {id: 4})" +
               ", (g {id: 5})" +
               ", (h {id: 6})" +
               ", (c)-[:REL {cost: 3.0}]->(d)" +
               ", (c)-[:REL {cost: 2.0}]->(e)" +
               ", (d)-[:REL {cost: 4.0}]->(f)" +
               ", (e)-[:REL {cost: 1.0}]->(d)" +
               ", (e)-[:REL {cost: 2.0}]->(f)" +
               ", (e)-[:REL {cost: 3.0}]->(g)" +
               ", (f)-[:REL {cost: 2.0}]->(g)" +
               ", (f)-[:REL {cost: 1.0}]->(h)" +
               ", (g)-[:REL {cost: 2.0}]->(h)";
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class
        );
        runQuery(createQuery());
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
        long sourceId = nodeIdByProperty(1);
        long targetId = nodeIdByProperty(6);

        if (!mapWrapper.containsKey(SOURCE_NODE_KEY)) {
            mapWrapper = mapWrapper.withNumber(SOURCE_NODE_KEY, sourceId);
        }
        if (!mapWrapper.containsKey(TARGET_NODE_KEY)) {
            mapWrapper = mapWrapper.withNumber(TARGET_NODE_KEY, targetId);
        }
        if (!mapWrapper.containsKey(K_KEY)) {
            mapWrapper = mapWrapper.withNumber(K_KEY, 3);
        }
        return mapWrapper;
    }

    long nodeIdByProperty(long propertyValue) {
        var nodeId = new MutableLong(0L);
        runQueryWithRowConsumer(
            formatWithLocale("MATCH (n) WHERE n.id = %d RETURN id(n) AS id", propertyValue),
            resultRow -> nodeId.setValue(resultRow.getNumber("id"))
        );
        return nodeId.longValue();
    }

    @Override
    public void assertResultEquals(DijkstraResult result1, DijkstraResult result2) {
        assertEquals(result1.pathSet(), result2.pathSet());
    }
}
