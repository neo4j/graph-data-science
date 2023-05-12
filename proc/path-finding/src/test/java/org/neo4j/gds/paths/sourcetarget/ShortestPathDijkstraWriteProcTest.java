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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestLogProvider;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.PathTestUtil.validationQuery;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ShortestPathDijkstraWriteProcTest extends BaseProcTest {

    long idA, idC, idD, idE, idF;
    static long[] ids0;
    static double[] costs0;

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

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathDijkstraWriteProc.class,
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
            .withRelationshipType("TYPE")
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
    void testWrite() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", true)
            .addParameter("writeCosts", true)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult(validationQuery(idA), List.of(Map.of("totalCost", 20.0D, "nodeIds", ids0, "costs", costs0)));
    }

    @ParameterizedTest
    @CsvSource(value = {"true,false", "false,true", "false,false"})
    void testWriteFlags(boolean writeNodeIds, boolean writeCosts) {
        var relationshipWeightProperty = "cost";


        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", writeNodeIds)
            .addParameter("writeCosts", writeCosts)
            .yields();

        runQuery(query);

        var validationQuery = "MATCH ()-[r:%s]->() RETURN r.nodeIds AS nodeIds, r.costs AS costs";
        var rowCount = runQueryWithRowConsumer(formatWithLocale(validationQuery, WRITE_RELATIONSHIP_TYPE), row -> {
            var nodeIds = row.get("nodeIds");
            var costs = row.get("costs");

            if (writeNodeIds) {
                assertThat(nodeIds).isNotNull();
            } else {
                assertThat(nodeIds).isNull();
            }

            if (writeCosts) {
                assertThat(costs).isNotNull();
            } else {
                assertThat(costs).isNull();
            }
        });
        assertThat(rowCount).isEqualTo(1);
    }

    @Test
    void testLazyComputationLoggingFinishes() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "BAR")
            .yields();

        runQuery(query);

        var messages = testLog.getMessages(TestLog.INFO);
        assertThat(messages.get(messages.size() - 1)).contains(":: Finished");
    }

    @Test
    void testProgressTracking() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "BAR")
            .yields();

        runQuery(query);

        var messages = testLog.getMessages(TestLog.INFO);

        assertThat(messages)
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .contains("Write shortest Paths :: WriteRelationshipStream :: Finished");

    }
}
