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
import org.neo4j.graphalgo.beta.paths.PathFactory;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.yens.Yens;
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.util.Arrays.asList;
import static org.neo4j.graphalgo.TestSupport.nodeIdByProperty;
import static org.neo4j.graphalgo.beta.paths.StreamResult.COST_PROPERTY_NAME;

class ShortestPathYensStreamProcTest extends ShortestPathYensProcTest<ShortestPathYensStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Yens, DijkstraResult, ShortestPathYensStreamConfig>> getProcedureClazz() {
        return ShortestPathYensStreamProc.class;
    }

    @Override
    public ShortestPathYensStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathYensStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void returnCorrectResult() {
        ShortestPathYensStreamConfig config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));
        String createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .graphCreate("graph")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.beta.shortestPath.yens")
            .streamMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("path", true)
            .yields();

        var idC = nodeIdByProperty(db, 1);
        var idD = nodeIdByProperty(db, 2);
        var idE = nodeIdByProperty(db, 3);
        var idF = nodeIdByProperty(db, 4);
        var idG = nodeIdByProperty(db, 5);
        var idH = nodeIdByProperty(db, 6);

        var ids0 = new long[]{idC, idE, idF, idH};
        var ids1 = new long[]{idC, idE, idG, idH};
        var ids2 = new long[]{idC, idD, idF, idH};

        var costs0 = new double[]{0.0, 2.0, 4.0, 5.0};
        var costs1 = new double[]{0.0, 2.0, 5.0, 7.0};
        var costs2 = new double[]{0.0, 3.0, 7.0, 8.0};

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            var path0 = PathFactory.create(tx, -1, ids0, costs0, RelationshipType.withName("PATH_0"), COST_PROPERTY_NAME);
            var path1 = PathFactory.create(tx, -4, ids1, costs1, RelationshipType.withName("PATH_1"), COST_PROPERTY_NAME);
            var path2 = PathFactory.create(tx, -7, ids2, costs2, RelationshipType.withName("PATH_2"), COST_PROPERTY_NAME);
            var expected = List.of(
                Map.of("index", 0L, "sourceNode", idC, "targetNode", idH, "totalCost", 5.0D, "costs", asList(costs0), "nodeIds", asList(ids0), "path", path0),
                Map.of("index", 1L, "sourceNode", idC, "targetNode", idH, "totalCost", 7.0D, "costs", asList(costs1), "nodeIds", asList(ids1), "path", path1),
                Map.of("index", 2L, "sourceNode", idC, "targetNode", idH, "totalCost", 8.0D, "costs", asList(costs2), "nodeIds", asList(ids2), "path", path2)
            );

            assertCypherResult(query, expected);
        });
    }
}
