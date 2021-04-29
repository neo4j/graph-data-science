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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
    void testStream() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.shortestPath.dijkstra")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("path", true)
            .yields();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            var expectedPath = PathFactory.create(
                tx,
                -1,
                ids0,
                costs0,
                RelationshipType.withName(formatWithLocale("PATH_0")), StreamResult.COST_PROPERTY_NAME
            );
            var expected = Map.of(
                "index", 0L,
                "sourceNode", idFunction.of("a"),
                "targetNode", idFunction.of("f"),
                "totalCost", 20.0D,
                "costs", Arrays.stream(costs0).boxed().collect(Collectors.toList()),
                "nodeIds", Arrays.stream(ids0).boxed().collect(Collectors.toList()),
                "path", expectedPath
            );

            assertCypherResult(query, List.of(expected));
        });
    }
}
