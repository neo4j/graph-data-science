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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DijkstraStreamProcTest extends DijkstraProcTest<DijkstraStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, DijkstraStreamConfig>> getProcedureClazz() {
        return DijkstraStreamProc.class;
    }

    @Override
    public DijkstraStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return DijkstraStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void returnCorrectResult() {
        DijkstraStreamConfig config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));
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

        runQueryWithRowConsumer(query, row -> {
            assertEquals(0, row.getNumber("index").longValue());

            assertEquals(nodeIdByProperty(0), row.getNumber("sourceNode").longValue());
            assertEquals(nodeIdByProperty(6), row.getNumber("targetNode").longValue());

            assertEquals(20.0D, row.getNumber("totalCost").doubleValue());

            assertEquals(List.of(0L, 2L, 4L, 3L, 5L), row.get("nodeIds"));
            assertEquals(List.of(0.0D, 2.0D, 5.0D, 9.0D, 20.0D), row.get("costs"));
        });
    }
}