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
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class KnnStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a { id: 1, knn: 1.0 } )" +
        ", (b { id: 2, knn: 2.0 } )" +
        ", (c { id: 3, knn: 5.0 } )" +
        ", (a)-[:IGNORE]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            KnnStatsProc.class,
            GraphProjectProc.class
        );
    }

    @Test
    void testStatsYields() {
        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeProperty("knn")
            .loadEverything()
            .yields();
        runQuery(createQuery);

        var query = GdsCypher
            .call("graph")
            .algo("gds.knn")
            .statsMode()
            .addParameter("sudo", true)
            .addParameter("nodeProperties", List.of("knn"))
            .addParameter("randomSeed", 42)
            .addParameter("concurrency", 1)
            .addParameter("topK", 1)
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("nodesCompared"))
                .asInstanceOf(LONG)
                .isEqualTo(3L);

            assertThat(row.getNumber("nodePairsConsidered"))
                .asInstanceOf(LONG)
                .isEqualTo(37L);

            assertThat(row.getBoolean("didConverge")).isTrue();

            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(1);

            assertThat(row.getNumber("similarityPairs"))
                .asInstanceOf(LONG)
                .isEqualTo(3);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row.get("similarityDistribution"))
                .asInstanceOf(MAP)
                .containsOnlyKeys("min", "max", "mean", "stdDev", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100")
                .allSatisfy((key, value) -> assertThat(value).asInstanceOf(DOUBLE).isGreaterThanOrEqualTo(0d));
        });

        assertThat(rowCount)
            .as("`stats` mode should always return one row")
            .isEqualTo(1);
    }
}
