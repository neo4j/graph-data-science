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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

class KmeansStreamProcTest extends BaseProcTest {


    public Class<? extends AlgoBaseProc<Kmeans, KmeansResult, KmeansStreamConfig, ?>> getProcedureClazz() {
        return KmeansStreamProc.class;
    }

    public KmeansStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return KmeansStreamConfig.of(mapWrapper);
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );
    }

    @AfterEach
    void cleanCatalog() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWork() {
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
            .algo("gds.beta.kmeans")
            .streamMode()
            .addParameter("k", 2)
            .addParameter("nodeProperty", "kmeans")
            .addParameter("randomSeed", 19)
            .addParameter("concurrency", 1)
            .addParameter("computeSilhouette", true)
            .yields("nodeId", "communityId", "distanceFromCentroid", "silhouette");
        assertCypherResult(algoQuery, List.of(
            Map.of("nodeId",
                0L,
                "communityId",
                0L,
                "distanceFromCentroid",
                0.5, "silhouette",
                0.9929292857150108
            ),
            Map.of(
                "nodeId",
                1L,
                "communityId",
                1L,
                "distanceFromCentroid",
                Math.sqrt(2),
                "silhouette",
                0.9799515133128792
            ),
            Map.of(
                "nodeId",
                2L,
                "communityId",
                0L,
                "distanceFromCentroid",
                0.5,
                "silhouette",
                0.9928938477702276
            ),
            Map.of(
                "nodeId",
                3L,
                "communityId",
                1L,
                "distanceFromCentroid",
                Math.sqrt(2),
                "silhouette",
                0.9799505034216922
            )

        ));
    }
    
}
