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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestLogProvider;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ShortestPathDijkstraStreamProcTest extends BaseProcTest {

    TestLog testLog;


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

    long idA, idC, idD, idE, idF;
    static long[] ids0;
    static double[] costs0;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathDijkstraStreamProc.class,
            GraphProjectProc.class
        );

        idA = idFunction.of("a");
        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");

        ids0 = new long[]{idA, idC, idE, idD, idF};
        costs0 = new double[]{0.0, 2.0, 5.0, 9.0, 20.0};

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = Neo4jProxy.testLog();
        builder.setUserLogProvider(new TestLogProvider(testLog));
    }

    @Test
    void testStream() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .streamMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "cost")
            .yields();

        runInTransaction(db, tx -> {
            PathFactory.RelationshipIds.set(0);

            var expectedPath = PathFactory.create(
                tx::getNodeById,
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
            PathFactory.RelationshipIds.set(0);
            assertCypherResult(query, List.of(expected));
        });
    }

    @Test
    void testLazyComputationLoggingFinishes() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .streamMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "cost")
            .yields();

        runInTransaction(db, tx -> runQueryWithoutClosingTheResult(tx, query, Map.of()).next());

        var messages = testLog.getMessages(TestLog.INFO);
        assertThat(messages.get(messages.size() - 1)).contains(":: Finished");
    }
}
