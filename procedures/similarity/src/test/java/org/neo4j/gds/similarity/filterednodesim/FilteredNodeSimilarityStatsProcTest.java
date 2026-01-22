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

class FilteredNodeSimilarityStatsProcTest extends BaseTest {

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
        GraphDatabaseApiProxy.registerProcedures(db, GraphProjectProc.class, FilteredNodeSimilarityStatsProc.class);
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
            .statsMode()
            .yields();
        runQuery(query, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "computeMillis",
                    "preProcessingMillis",
                    "postProcessingMillis",
                    "nodesCompared",
                    "similarityPairs",
                    "similarityDistribution",
                    "configuration"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(4L);
                assertThat(resultRow.get("similarityPairs"))
                    .as("similarityPairs")
                    .asInstanceOf(LONG)
                    .isEqualTo(10L);

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
                assertThat(resultRow.get("postProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("postProcessingMillis")
                    // TODO: postProcessingMillis is not getting set
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
            .statsMode()
            .addParameter("sourceNodeFilter", filter)
            .yields();

        runQuery(sourceFilteredQuery, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "computeMillis",
                    "preProcessingMillis",
                    "postProcessingMillis",
                    "similarityPairs",
                    "similarityDistribution",
                    "configuration",
                    "nodesCompared"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(2);
                assertThat(resultRow.get("similarityPairs"))
                    .as("similarityPairs")
                    .asInstanceOf(LONG)
                    .isEqualTo(6L);

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
                assertThat(resultRow.get("postProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("postProcessingMillis")
                    // TODO: postProcessingMillis is not getting set
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
    void shouldWorkWithTargetFiltering() {
        var filter = List.of(0, 3);

        var targetFilteredQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity.filtered")
            .statsMode()
            .addParameter("targetNodeFilter", filter)
            .yields();

        runQuery(targetFilteredQuery, result -> {
            assertThat(result.columns())
                .containsExactlyInAnyOrder(
                    "computeMillis",
                    "preProcessingMillis",
                    "postProcessingMillis",
                    "similarityPairs",
                    "similarityDistribution",
                    "configuration",
                    "nodesCompared"
                );

            while (result.hasNext()) {
                var resultRow = result.next();
                assertThat(resultRow.get("nodesCompared"))
                    .asInstanceOf(LONG)
                    .as("nodesCompared")
                    .isEqualTo(4);
                assertThat(resultRow.get("similarityPairs"))
                    .as("similarityPairs")
                    .asInstanceOf(LONG)
                    .isEqualTo(6L);

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

                assertThat(resultRow.get("configuration"))
                    .isNotNull()
                    .asInstanceOf(MAP)
                    .isNotEmpty();
            }
            return true;
        });
    }
}
