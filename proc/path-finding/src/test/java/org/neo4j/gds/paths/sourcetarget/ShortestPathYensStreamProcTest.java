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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.util.Arrays.asList;
import static org.neo4j.gds.config.SourceNodeConfig.SOURCE_NODE_KEY;
import static org.neo4j.gds.config.TargetNodeConfig.TARGET_NODE_KEY;

class ShortestPathYensStreamProcTest extends BaseProcTest {
    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
        "  (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (g:Label)" +
        ", (h:Label)" +
        ", (c)-[:TYPE {cost: 2.0}]->(e)" +
        ", (c)-[:TYPE {cost: 3.0}]->(h)" +
        ", (e)-[:TYPE {cost: 2.0}]->(f)" +
        ", (e)-[:TYPE {cost: 3.0}]->(g)" +
        ", (e)-[:TYPE {cost: 1.0}]->(h)" +
        ", (f)-[:TYPE {cost: 1.0}]->(d)" +
        ", (f)-[:TYPE {cost: 2.0}]->(g)" +
        ", (g)-[:TYPE {cost: 2.0}]->(d)" +
        ", (h)-[:TYPE {cost: 4.0}]->(f)";

    protected static final String GRAPH_NAME = "graph";
    private static final String K_KEY = "k";
    long idC;
    long idH;
    long idD;
    long idE;
    long idF;
    long idG;
    long[] ids0;
    long[] ids1;
    long[] ids2;
    double[] costs0;
    double[] costs1;
    double[] costs2;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathYensStreamProc.class,
            GraphProjectProc.class
        );

        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");
        idG = idFunction.of("g");
        idH = idFunction.of("h");

        ids0 = new long[]{idC, idE, idF, idD};
        ids1 = new long[]{idC, idE, idG, idD};
        ids2 = new long[]{idC, idH, idF, idD};

        costs0 = new double[]{0.0, 2.0, 4.0, 5.0};
        costs1 = new double[]{0.0, 2.0, 5.0, 7.0};
        costs2 = new double[]{0.0, 3.0, 7.0, 8.0};

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @Test
    void testStream() {
        var config = ShortestPathYensStreamConfig.of(CypherMapWrapper.empty()
            .withNumber(SOURCE_NODE_KEY, idC)
            .withNumber(TARGET_NODE_KEY, idD)
            .withNumber(K_KEY, 3)
        );

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
            var path0 = PathFactory.create(
                tx::getNodeById,
                ids0,
                costs0,
                RelationshipType.withName("PATH_0"),
                StreamResult.COST_PROPERTY_NAME
            );
            var path1 = PathFactory.create(
                tx::getNodeById,
                ids1,
                costs1,
                RelationshipType.withName("PATH_1"),
                StreamResult.COST_PROPERTY_NAME
            );
            var path2 = PathFactory.create(
                tx::getNodeById,
                ids2,
                costs2,
                RelationshipType.withName("PATH_2"),
                StreamResult.COST_PROPERTY_NAME
            );
            var expected = List.of(
                Map.of(
                    "index",
                    0L,
                    "sourceNode",
                    idC,
                    "targetNode",
                    idD,
                    "totalCost",
                    5.0D,
                    "costs",
                    asList(costs0),
                    "nodeIds",
                    asList(ids0),
                    "path",
                    path0
                ),
                Map.of(
                    "index",
                    1L,
                    "sourceNode",
                    idC,
                    "targetNode",
                    idD,
                    "totalCost",
                    7.0D,
                    "costs",
                    asList(costs1),
                    "nodeIds",
                    asList(ids1),
                    "path",
                    path1
                ),
                Map.of(
                    "index",
                    2L,
                    "sourceNode",
                    idC,
                    "targetNode",
                    idD,
                    "totalCost",
                    8.0D,
                    "costs",
                    asList(costs2),
                    "nodeIds",
                    asList(ids2),
                    "path",
                    path2
                )
            );
            PathFactory.RelationshipIds.set(0);
            assertCypherResult(query, expected);
        });
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
