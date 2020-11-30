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
import org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class ShortestPathDijkstraStreamProcTest extends ShortestPathDijkstraProcTest<ShortestPathDijkstraStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, ShortestPathDijkstraStreamConfig>> getProcedureClazz() {
        return ShortestPathDijkstraStreamProc.class;
    }

    @Override
    public ShortestPathDijkstraStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathDijkstraStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void returnCorrectResult() {
        ShortestPathDijkstraStreamConfig config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));
        String createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .graphCreate("graph")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.shortestPath.dijkstra")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", "cost")
            .yields();

        var idA = nodeIdByProperty(1);
        var idC = nodeIdByProperty(3);
        var idD = nodeIdByProperty(4);
        var idE = nodeIdByProperty(5);
        var idF = nodeIdByProperty(6);

        var expected = Map.of(
            "index", 0L,
            "sourceNode", nodeIdByProperty(1),
            "targetNode", nodeIdByProperty(6),
            "totalCost", 20.0D,
            "costs", List.of(0.0, 2.0, 5.0, 9.0, 20.0),
            "nodeIds", List.of(idA, idC, idE, idD, idF)
        );

        assertCypherResult(query, List.of(expected));
    }
}