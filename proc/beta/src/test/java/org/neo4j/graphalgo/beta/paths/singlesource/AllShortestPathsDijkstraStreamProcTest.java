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
package org.neo4j.graphalgo.beta.paths.singlesource;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.beta.paths.PathFactory;
import org.neo4j.graphalgo.beta.paths.dijkstra.Dijkstra;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.graphalgo.beta.paths.StreamResult.COST_PROPERTY_NAME;

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
    void returnCorrectResult() throws TransactionFailureException {
        AllShortestPathsDijkstraStreamConfig config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));
        String createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .graphCreate("graph")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.allShortestPaths.dijkstra")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("path", true)
            .yields();

        var idA = nodeIdByProperty(1);
        var idB = nodeIdByProperty(2);
        var idC = nodeIdByProperty(3);
        var idD = nodeIdByProperty(4);
        var idE = nodeIdByProperty(5);
        var idF = nodeIdByProperty(6);

        var costs0 = List.of(0.0);
        var costs1 = List.of(0.0, 2.0 );
        var costs2 = List.of(0.0, 4.0);
        var costs3 = List.of(0.0, 2.0, 5.0);
        var costs4 = List.of(0.0, 2.0, 5.0, 9.0);
        var costs5 = List.of(0.0, 2.0, 5.0, 9.0, 20.0);

        var ids0 = List.of(idA);
        var ids1 = List.of(idA, idC);
        var ids2 = List.of(idA, idB);
        var ids3 = List.of(idA, idC, idE);
        var ids4 = List.of(idA, idC, idE, idD);
        var ids5 = List.of(idA, idC, idE, idD, idF);

        //@formatter:off
        var ktx = GraphDatabaseApiProxy.newKernelTransaction(db).ktx();
        var path0 = PathFactory.create(ktx, -1, ids0, costs0, RelationshipType.withName("PATH_0"), COST_PROPERTY_NAME);
        var path1 = PathFactory.create(ktx, -1, ids1, costs1, RelationshipType.withName("PATH_1"), COST_PROPERTY_NAME);
        var path2 = PathFactory.create(ktx, -2, ids2, costs2, RelationshipType.withName("PATH_2"), COST_PROPERTY_NAME);
        var path3 = PathFactory.create(ktx, -3, ids3, costs3, RelationshipType.withName("PATH_3"), COST_PROPERTY_NAME);
        var path4 = PathFactory.create(ktx, -5, ids4, costs4, RelationshipType.withName("PATH_4"), COST_PROPERTY_NAME);
        var path5 = PathFactory.create(ktx, -8, ids5, costs5, RelationshipType.withName("PATH_5"), COST_PROPERTY_NAME);
        ktx.close();

        var expected = List.of(
            Map.of("index", 0L, "sourceNode", idA, "targetNode", idA, "totalCost", 0.0D, "costs", costs0, "nodeIds", ids0, "path", path0),
            Map.of("index", 1L, "sourceNode", idA, "targetNode", idC, "totalCost", 2.0D, "costs", costs1, "nodeIds", ids1, "path", path1),
            Map.of("index", 2L, "sourceNode", idA, "targetNode", idB, "totalCost", 4.0D, "costs", costs2, "nodeIds", ids2, "path", path2),
            Map.of("index", 3L, "sourceNode", idA, "targetNode", idE, "totalCost", 5.0D, "costs", costs3, "nodeIds", ids3, "path", path3),
            Map.of("index", 4L, "sourceNode", idA, "targetNode", idD, "totalCost", 9.0D, "costs", costs4, "nodeIds", ids4, "path", path4),
            Map.of("index", 5L, "sourceNode", idA, "targetNode", idF, "totalCost", 20.0D, "costs", costs5, "nodeIds", ids5, "path", path5)
        );
        //@formatter:on

        assertCypherResult(query, expected);
    }
}
