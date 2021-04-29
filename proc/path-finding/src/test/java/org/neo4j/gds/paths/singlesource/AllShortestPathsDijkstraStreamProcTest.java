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
package org.neo4j.gds.paths.singlesource;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class AllShortestPathsDijkstraStreamProcTest extends AllShortestPathsDijkstraProcTest<AllShortestPathsDijkstraStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, AllShortestPathsDijkstraStreamConfig>> getProcedureClazz() {
        return AllShortestPathsDijkstraStreamProc.class;
    }

    @Override
    public AllShortestPathsDijkstraStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return AllShortestPathsDijkstraStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void returnCorrectResult() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.allShortestPaths.dijkstra")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("path", true)
            .yields();

        //@formatter:off
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            var path0 = PathFactory.create(tx, -1, ids0, costs0, RelationshipType.withName("PATH_0"), StreamResult.COST_PROPERTY_NAME);
            var path1 = PathFactory.create(tx, -1, ids1, costs1, RelationshipType.withName("PATH_1"), StreamResult.COST_PROPERTY_NAME);
            var path2 = PathFactory.create(tx, -2, ids2, costs2, RelationshipType.withName("PATH_2"), StreamResult.COST_PROPERTY_NAME);
            var path3 = PathFactory.create(tx, -3, ids3, costs3, RelationshipType.withName("PATH_3"), StreamResult.COST_PROPERTY_NAME);
            var path4 = PathFactory.create(tx, -5, ids4, costs4, RelationshipType.withName("PATH_4"), StreamResult.COST_PROPERTY_NAME);
            var path5 = PathFactory.create(tx, -8, ids5, costs5, RelationshipType.withName("PATH_5"), StreamResult.COST_PROPERTY_NAME);
            var expected = List.of(
                Map.of("index", 0L, "sourceNode", idA, "targetNode", idA, "totalCost", 0.0D, "costs", Collections.singletonList(costs0), "nodeIds", Collections.singletonList(ids0), "path", path0),
                Map.of("index", 1L, "sourceNode", idA, "targetNode", idC, "totalCost", 2.0D, "costs", Collections.singletonList(costs1), "nodeIds", Collections.singletonList(ids1), "path", path1),
                Map.of("index", 2L, "sourceNode", idA, "targetNode", idB, "totalCost", 4.0D, "costs", Collections.singletonList(costs2), "nodeIds", Collections.singletonList(ids2), "path", path2),
                Map.of("index", 3L, "sourceNode", idA, "targetNode", idE, "totalCost", 5.0D, "costs", Collections.singletonList(costs3), "nodeIds", Collections.singletonList(ids3), "path", path3),
                Map.of("index", 4L, "sourceNode", idA, "targetNode", idD, "totalCost", 9.0D, "costs", Collections.singletonList(costs4), "nodeIds", Collections.singletonList(ids4), "path", path4),
                Map.of("index", 5L, "sourceNode", idA, "targetNode", idF, "totalCost", 20.0D, "costs", Collections.singletonList(costs5), "nodeIds", Collections.singletonList(ids5), "path", path5)
            );
            assertCypherResult(query, expected);
        });
        //@formatter:on

    }
}
