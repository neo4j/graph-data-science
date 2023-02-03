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
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.util.Arrays.asList;

class ShortestPathYensStreamProcTest extends ShortestPathYensProcTest<ShortestPathYensStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Yens, DijkstraResult, ShortestPathYensStreamConfig, ?>> getProcedureClazz() {
        return ShortestPathYensStreamProc.class;
    }

    @Override
    public ShortestPathYensStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathYensStreamConfig.of(mapWrapper);
    }

    @Test
    void testStream() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.yens")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", "cost")
            .yields();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            PathFactory.RelationshipIds.set(0);
            var path0 = PathFactory.create(tx::getNodeById, ids0, costs0, RelationshipType.withName("PATH_0"), StreamResult.COST_PROPERTY_NAME);
            var path1 = PathFactory.create(tx::getNodeById, ids1, costs1, RelationshipType.withName("PATH_1"), StreamResult.COST_PROPERTY_NAME);
            var path2 = PathFactory.create(tx::getNodeById, ids2, costs2, RelationshipType.withName("PATH_2"), StreamResult.COST_PROPERTY_NAME);
            var expected = List.of(
                Map.of("index", 0L, "sourceNode", idC, "targetNode", idD, "totalCost", 5.0D, "costs", asList(costs0), "nodeIds", asList(ids0), "path", path0),
                Map.of("index", 1L, "sourceNode", idC, "targetNode", idD, "totalCost", 7.0D, "costs", asList(costs1), "nodeIds", asList(ids1), "path", path1),
                Map.of("index", 2L, "sourceNode", idC, "targetNode", idD, "totalCost", 8.0D, "costs", asList(costs2), "nodeIds", asList(ids2), "path", path2)
            );
            PathFactory.RelationshipIds.set(0);
            assertCypherResult(query, expected);
        });
    }
}
