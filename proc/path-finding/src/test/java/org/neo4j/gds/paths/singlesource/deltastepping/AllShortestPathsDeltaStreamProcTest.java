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
package org.neo4j.gds.paths.singlesource.deltastepping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.singlesource.delta.AllShortestPathsDeltaStreamProc;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.util.Arrays.asList;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.registerProcedures;

class AllShortestPathsDeltaStreamProcTest extends BaseTest {

    protected static final String GRAPH_NAME = "graph";

    long idA, idB, idC, idD, idE, idF;
    static double[] costs0, costs1, costs2, costs3, costs4, costs5;
    static long[] ids0, ids1, ids2, ids3, ids4, ids5;

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE" +
                                            "  (:Offset)" +
                                            ", (a:Label)" +
                                            ", (b:Label)" +
                                            ", (c:Label)" +
                                            ", (d:Label)" +
                                            ", (e:Label)" +
                                            ", (f:Label)" +
                                            ", (a)-[:TYPE {cost: 4}]->(b)" +
                                            ", (a)-[:TYPE {cost: 2}]->(c)" +
                                            ", (b)-[:TYPE {cost: 5}]->(c)" +
                                            ", (b)-[:TYPE {cost: 10}]->(d)" +
                                            ", (c)-[:TYPE {cost: 3}]->(e)" +
                                            ", (d)-[:TYPE {cost: 11}]->(f)" +
                                            ", (e)-[:TYPE {cost: 4}]->(d)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            db,
            AllShortestPathsDeltaStreamProc.class,
            GraphProjectProc.class
        );

        idA = idFunction.of("a");
        idB = idFunction.of("b");
        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");

        costs0 = new double[]{0.0};
        costs1 = new double[]{0.0, 4.0};
        costs2 = new double[]{0.0, 2.0};
        costs3 = new double[]{0.0, 2.0, 5.0, 9.0};
        costs4 = new double[]{0.0, 2.0, 5.0};
        costs5 = new double[]{0.0, 2.0, 5.0, 9.0, 20.0};

        ids0 = new long[]{idA};
        ids1 = new long[]{idA, idB};
        ids2 = new long[]{idA, idC};
        ids3 = new long[]{idA, idC, idE, idD};
        ids4 = new long[]{idA, idC, idE};
        ids5 = new long[]{idA, idC, idE, idD, idF};

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @Test
    void returnCorrectResult() {
        long sourceId = idFunction.of("a");

        var query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.allShortestPaths.delta")
            .streamMode()
            .addParameter("sourceNode", sourceId)
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("concurrency", 1)
            .yields();

        //@formatter:off
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            PathFactory.RelationshipIds.set(0);
            var path0 = PathFactory.create(tx, ids0, costs0, RelationshipType.withName("PATH_0"), StreamResult.COST_PROPERTY_NAME);
            var path1 = PathFactory.create(tx, ids1, costs1, RelationshipType.withName("PATH_1"), StreamResult.COST_PROPERTY_NAME);
            var path2 = PathFactory.create(tx, ids2, costs2, RelationshipType.withName("PATH_2"), StreamResult.COST_PROPERTY_NAME);
            var path3 = PathFactory.create(tx, ids3, costs3, RelationshipType.withName("PATH_3"), StreamResult.COST_PROPERTY_NAME);
            var path4 = PathFactory.create(tx, ids4, costs4, RelationshipType.withName("PATH_4"), StreamResult.COST_PROPERTY_NAME);
            var path5 = PathFactory.create(tx, ids5, costs5, RelationshipType.withName("PATH_5"), StreamResult.COST_PROPERTY_NAME);
            var expected = List.of(
                Map.of("index", 0L, "sourceNode", idA, "targetNode", idA, "totalCost", 0.0D, "costs", asList(costs0), "nodeIds", asList(ids0), "path", path0),
                Map.of("index", 1L, "sourceNode", idA, "targetNode", idB, "totalCost", 4.0D, "costs", asList(costs1), "nodeIds", asList(ids1), "path", path1),
                Map.of("index", 2L, "sourceNode", idA, "targetNode", idC, "totalCost", 2.0D, "costs", asList(costs2), "nodeIds", asList(ids2), "path", path2),
                Map.of("index", 3L, "sourceNode", idA, "targetNode", idD, "totalCost", 9.0D, "costs", asList(costs3), "nodeIds", asList(ids3), "path", path3),
                Map.of("index", 4L, "sourceNode", idA, "targetNode", idE, "totalCost", 5.0D, "costs", asList(costs4), "nodeIds", asList(ids4), "path", path4),
                Map.of("index", 5L, "sourceNode", idA, "targetNode", idF, "totalCost", 20.0D, "costs", asList(costs5), "nodeIds", asList(ids5), "path", path5)
            );
            PathFactory.RelationshipIds.set(0);
            assertCypherResult(query, expected);
        });
        //@formatter:on

    }

}
