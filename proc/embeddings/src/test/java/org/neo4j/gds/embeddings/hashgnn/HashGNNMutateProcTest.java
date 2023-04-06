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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class HashGNNMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:N {f1: 1, f2: [0.0, 0.0]})" +
        ", (b:N {f1: 0, f2: [1.0, 0.0]})" +
        ", (c:N {f1: 0, f2: [0.0, 1.0]})" +
        ", (b)-[:R1]->(a)" +
        ", (b)-[:R2]->(c)";

    static String expectedMutatedGraph = "CREATE" +
                                         "  (a:N {f1: 1, f2: [0.0, 0.0], embedding: [1.0, 0.0, 0.0]})" +
                                         ", (b:N {f1: 0, f2: [1.0, 0.0], embedding: [1.0, 0.0, 1.0]})" +
                                         ", (c:N {f1: 0, f2: [0.0, 1.0], embedding: [0.0, 0.0, 1.0]})" +
                                         ", (b)-[:R1]->(a)" +
                                         ", (b)-[:R2]->(c)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            HashGNNMutateProc.class,
            GraphProjectProc.class
        );

        String graphCreateQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("N")
            .withNodeProperty("f1")
            .withNodeProperty("f2")
            .withRelationshipType("R1")
            .withRelationshipType("R2")
            .yields();
        runQuery(graphCreateQuery);
    }

    @Test
    void shouldMutate() {
        var query = GdsCypher.call("graph").algo("gds.beta.hashgnn")
            .mutateMode().addParameter("heterogeneous", true)
            .addParameter("iterations", 2)
            .addParameter("embeddingDensity", 2)
            .addParameter("randomSeed", 42L)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .addParameter("mutateProperty", "embedding")
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("mutateMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(3L);

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(3L);

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);
        });

        assertThat(rowCount).isEqualTo(1L);

        Graph mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore().getUnion();
        TestGraph expected = fromGdl(expectedMutatedGraph);
        assertGraphEquals(expected, mutatedGraph);
    }

}
