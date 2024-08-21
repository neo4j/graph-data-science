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
package org.neo4j.gds.kmeans;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class KmeansStreamProcTest extends BaseProcTest {
    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            KmeansStreamProc.class,
            GraphProjectProc.class
        );
    }

    @AfterEach
    void cleanCatalog() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.kmeans","gds.beta.kmeans"})
    void shouldStream(String procedureName) {
        String nodeCreateQuery =
            "CREATE" +
            "  (a:Person {kmeans: [1.0, 1.0]} )" +
            "  ,(c:Person {kmeans: [102.0, 100.0]} )" +
            "  ,(b:Person {kmeans: [1.0, 2.0]} )" +
            "  ,(d:Person {kmeans: [100.0, 102.0]} )";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeProperties(List.of("kmeans"), DefaultValue.of(new double[]{0.0f}))
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo(procedureName)
            .streamMode()
            .addParameter("k", 2)
            .addParameter("nodeProperty", "kmeans")
            .addParameter("randomSeed", 19)
            .addParameter("concurrency", 1)
            .addParameter("computeSilhouette", true)
            .yields("nodeId", "communityId", "distanceFromCentroid", "silhouette");

        var expectedStreamResult = Map.of(
            0L, new KmeansTestStreamResult(0L, 0.5, 0.9929292857150108),
            1L, new KmeansTestStreamResult(1L, Math.sqrt(2), 0.9799515133128792),
            2L, new KmeansTestStreamResult(0L, 0.5, 0.9928938477702276),
            3L, new KmeansTestStreamResult(1L, Math.sqrt(2), 0.9799505034216922)

        );

        var rowCount = runQueryWithRowConsumer(algoQuery, (resultRow) -> {

            var nodeId = resultRow.getNumber("nodeId");
            var expectedCommunity = expectedStreamResult.get(nodeId).communityId;
            var expectedDistance = expectedStreamResult.get(nodeId).distanceFromCentroid;
            var expectedsilhouette = expectedStreamResult.get(nodeId).silhouette;

            assertThat(resultRow.getNumber("communityId")).asInstanceOf(LONG).isEqualTo(expectedCommunity);
            assertThat(resultRow.getNumber("distanceFromCentroid")).asInstanceOf(DOUBLE).isCloseTo(
                expectedDistance,
                Offset.offset(1e-6)
            );
            assertThat(resultRow.getNumber("silhouette")).asInstanceOf(DOUBLE).isCloseTo(
                expectedsilhouette,
                Offset.offset(1e-6)
            );

        });
        assertThat(rowCount).isEqualTo(4l);
    }

    class KmeansTestStreamResult {

        public long communityId;
        public double distanceFromCentroid;
        public double silhouette;

        public KmeansTestStreamResult(long communityId, double distanceFromCentroid, double silhouette) {
            this.communityId = communityId;
            this.distanceFromCentroid = distanceFromCentroid;
            this.silhouette = silhouette;
        }
    }
    
}
