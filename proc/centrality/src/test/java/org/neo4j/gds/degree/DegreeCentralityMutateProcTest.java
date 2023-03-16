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
package org.neo4j.gds.degree;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.TestSupport.fromGdl;


class DegreeCentralityMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +
        ", (h:Label1)" +
        ", (i:Label1)" +
        ", (j:Label1)" +

        ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +

        ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

        ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 2.0}]->(e)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            DegreeCentralityMutateProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call("dcGraph")
                .graphProject()
                .loadEverything()
                .yields()
        );
    }

    @Test
    void testMutate() {
        String query = GdsCypher
            .call("dcGraph")
            .algo("degree")
            .mutateMode()
            .addParameter("mutateProperty", "degreeScore")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("centralityDistribution"))
                .isNotNull()
                .isInstanceOf(Map.class)
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsEntry("min", 0.0)
                .hasEntrySatisfying("max",
                    value -> assertThat(value)
                        .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                        .isEqualTo(3.0, Offset.offset(1e-4))
                );

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("mutateMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(10L);
        });

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "dcGraph")
            .graphStore()
            .getUnion();

        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), actualGraph);

    }

    private String expectedMutatedGraph() {
        return "CREATE" +
               "  (a { degreeScore: 0.0})" +
               ", (b { degreeScore: 1.0})" +
               ", (c { degreeScore: 1.0})" +
               ", (d { degreeScore: 2.0})" +
               ", (e { degreeScore: 3.0})" +
               ", (f { degreeScore: 2.0})" +
               ", (g { degreeScore: 0.0})" +
               ", (h { degreeScore: 0.0})" +
               ", (i { degreeScore: 0.0})" +
               ", (j { degreeScore: 0.0})" +

               ", (b)-[{weight: 1.0}]->(c)" +

               ", (c)-[{weight: 1.0}]->(b)" +

               ", (d)-[{weight: 1.0}]->(a)" +
               ", (d)-[{weight: 1.0}]->(b)" +

               ", (e)-[{weight: 1.0}]->(b)" +
               ", (e)-[{weight: 1.0}]->(d)" +
               ", (e)-[{weight: 1.0}]->(f)" +

               ", (f)-[{weight: 1.0}]->(b)" +
               ", (f)-[{weight: 1.0}]->(e)";
    }

}
