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
import org.neo4j.graphalgo.beta.paths.astar.AStar;
import org.neo4j.graphalgo.beta.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.graphalgo.beta.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.beta.paths.PathTestUtil.validationQuery;
import static org.neo4j.graphalgo.beta.paths.astar.config.ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY;
import static org.neo4j.graphalgo.beta.paths.astar.config.ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY;
import static org.neo4j.graphalgo.config.WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY;

class ShortestPathAStarWriteProcTest extends ShortestPathAStarProcTest<ShortestPathAStarWriteConfig> {

    @Override
    public Class<? extends AlgoBaseProc<AStar, DijkstraResult, ShortestPathAStarWriteConfig>> getProcedureClazz() {
        return ShortestPathAStarWriteProc.class;
    }

    @Override
    public ShortestPathAStarWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathAStarWriteConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
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
    void testWrite() {
        var relationshipWeightProperty = "cost";

        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.shortestPath.astar")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter(LATITUDE_PROPERTY_KEY, config.latitudeProperty())
            .addParameter(LONGITUDE_PROPERTY_KEY, config.longitudeProperty())
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult(validationQuery(idA), List.of(Map.of("totalCost", 2979.0D, "nodeIds", ids0, "costs", costs0)));
    }
}
