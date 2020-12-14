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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.beta.paths.dijkstra.Dijkstra;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphalgo.TestSupport.nodeIdByProperty;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.COSTS_KEY;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.NODE_IDS_KEY;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;
import static org.neo4j.graphalgo.config.WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class ShortestPathDijkstraWriteProcTest extends ShortestPathDijkstraProcTest<ShortestPathDijkstraWriteConfig> {

    private static final String WRITE_RELATIONSHIP_TYPE = "PATH";

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig>> getProcedureClazz() {
        return ShortestPathDijkstraWriteProc.class;
    }

    @Override
    public ShortestPathDijkstraWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathDijkstraWriteConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(WRITE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Test
    void testWriteYields() {
        var relationshipWeightProperty = "cost";

        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty(relationshipWeightProperty)
            .graphCreate("graph")
            .yields();
        runQuery(createQuery);

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        runQueryWithRowConsumer(
            query,
            row -> {
                assertUserInput(row, "writeRelationshipType", WRITE_RELATIONSHIP_TYPE);
                assertUserInput(row, "relationshipWeightProperty", relationshipWeightProperty);
                assertEquals(1L, row.getNumber("relationshipsWritten"));
                assertNotEquals(-1L, row.getNumber("createMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("writeMillis"));
            }
        );

        var validationQuery = formatWithLocale(
            "MATCH ({ id: %d })-[r:%s]->({ id: %d }) RETURN r.%s AS totalCost, r.%s AS nodeIds, r.%s AS costs",
            1,
            WRITE_RELATIONSHIP_TYPE,
            6,
            TOTAL_COST_KEY,
            NODE_IDS_KEY,
            COSTS_KEY
        );

        var idA = nodeIdByProperty(db, 1);
        var idC = nodeIdByProperty(db, 3);
        var idD = nodeIdByProperty(db, 4);
        var idE = nodeIdByProperty(db, 5);
        var idF = nodeIdByProperty(db, 6);

        assertCypherResult(validationQuery, List.of(Map.of(
            "totalCost", 20.0D,
            "nodeIds", new long[]{idA, idC, idE, idD, idF},
            "costs", new double[]{0.0, 2.0, 5.0, 9.0, 20.0}
            ))
        );
    }
}
