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
package org.neo4j.gds.similarity.filterednodesim;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class FilteredNodeSimilarityMutateProcTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person)" +
        ", (b:Person)" +
        ", (c:Person)" +
        ", (d:Person)" +
        ", (i1:Item)" +
        ", (i2:Item)" +
        ", (i3:Item)" +
        ", (i4:Item)" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)" +
        ", (d)-[:LIKES]->(i1)" +
        ", (d)-[:LIKES]->(i2)" +
        ", (d)-[:LIKES]->(i3)";

    @BeforeEach
    void setup() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, GraphProjectProc.class, FilteredNodeSimilarityMutateProc.class);
        runQuery(DB_CYPHER);
        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabels("Person", "Item")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);
    }

    @Test
    void shouldWorkWithoutFiltering() {
        var query = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity.filtered")
            .mutateMode()
            .addParameter("mutateProperty", "score")
            .addParameter("mutateRelationshipType", "SIMILAR_TO")
            .yields();
        runQuery(query, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "preProcessingMillis",
                    "postProcessingMillis",
                    "mutateMillis",
                    "computeMillis",
                    "nodesCompared",
                    "relationshipsWritten",
                    "similarityDistribution",
                    "configuration"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("relationshipsWritten"))
                    .asInstanceOf(LONG)
                    .as("relationshipsWritten")
                    .isEqualTo(10);
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(4L);

                assertThat(resultRow.get("similarityDistribution"))
                    .isNotNull()
                    .asInstanceOf(MAP)
                    .isNotEmpty();

                assertThat(resultRow.get("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("preProcessingMillis")
                    .isGreaterThanOrEqualTo(0);
                assertThat(resultRow.get("computeMillis"))
                    .asInstanceOf(LONG)
                    .as("computeMillis")
                    .isGreaterThanOrEqualTo(0);
                assertThat(resultRow.get("mutateMillis"))
                    .asInstanceOf(LONG)
                    .as("mutateMillis")
                    .isGreaterThanOrEqualTo(0);

                assertThat(resultRow.get("configuration"))
                    .isNotNull()
                    .asInstanceOf(MAP)
                    .isNotEmpty();
            }

            return true;
        });
    }

    @Test
    void shouldWorkWithSourceFiltering() {
        var filter = List.of(0, 3);

        var sourceFilteredQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity.filtered")
            .mutateMode()
            .addParameter("mutateProperty", "score")
            .addParameter("mutateRelationshipType", "SIMILAR_TO")
            .addParameter("sourceNodeFilter", filter)
            .yields("relationshipsWritten", "nodesCompared");

        runQuery(sourceFilteredQuery, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "nodesCompared",
                    "relationshipsWritten"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("relationshipsWritten"))
                    .asInstanceOf(LONG)
                    .as("relationshipsWritten")
                    .isEqualTo(6);
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(2);
            }
            return true;
        });
    }

    @Test
    void shouldWorkWithTargetFiltering() {
        var filter = List.of(0, 3);

        var targetFilteredQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity.filtered")
            .mutateMode()
            .addParameter("mutateProperty", "score")
            .addParameter("mutateRelationshipType", "SIMILAR_TO")
            .addParameter("targetNodeFilter", filter)
            .yields("relationshipsWritten", "nodesCompared");

        runQuery(targetFilteredQuery, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "nodesCompared",
                    "relationshipsWritten"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("relationshipsWritten"))
                    .asInstanceOf(LONG)
                    .as("relationshipsWritten")
                    .isEqualTo(6);
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(4);
            }
            return true;
        });
    }
}
