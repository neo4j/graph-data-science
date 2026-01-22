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
package org.neo4j.gds.approxmaxkcut;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.community.ApproxMaxKCutWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.procedure.Mode.WRITE;

class ApproxMaxKCutWriteProcTest extends BaseProcTest {

    public static class ApproxMaxKCutWriteProc extends BaseProc {

        @Context
        public GraphDataScienceProcedures facade;

        @Procedure(name = "gds.maxkcut.write", mode = WRITE)
        public Stream<ApproxMaxKCutWriteResult> foo(
            @Name(value = "graphName") String graphName,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return facade.algorithms().community().approxMaxKCutWrite(graphName, configuration);
        }
    }

    @Neo4jGraph
    private static final
    String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(e)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (d)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (f)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)";

    static final String GRAPH_NAME = "myGraph";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            ApproxMaxKCutWriteProc.class,
            GraphProjectProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @Test
    void write() {
        String query = GdsCypher
            .call(GRAPH_NAME)
            .algo("maxkcut")
            .writeMode()
            .addParameter("k", 2)
            .addParameter("iterations", 8)
            .addParameter("vnsMaxNeighborhoodOrder", 0)
            .addParameter("concurrency", 1)
            .addParameter("randomSeed", 1337L)
            .addParameter("writeProperty", "community").yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {

            assertThat(row.getNumber("cutCost"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(13.0);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("writeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(7L);
        });

        assertThat(rowCount).isEqualTo(1L);

        var sideEffectCheckingQuery = "MATCH (n) RETURN n.community AS prop";
        var postWriteRowCount = runQueryWithRowConsumer(sideEffectCheckingQuery, row -> {
            assertThat(row.getNumber("prop")).asInstanceOf(LONG).isNotNegative();
        });

        assertThat(postWriteRowCount).isEqualTo(7);

        testLog.assertContainsMessage(TestLog.INFO, "ApproxMaxKCut :: Finished");
    }


    @AfterEach
    void clearStore() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
