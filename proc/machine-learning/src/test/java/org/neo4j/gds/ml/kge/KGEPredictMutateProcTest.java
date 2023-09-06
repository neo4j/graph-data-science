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
package org.neo4j.gds.ml.kge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KGEPredictMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    static String DB_QUERY =
        "CREATE " +
            "  (n0:N {a: 1.0, emb: [0.0, 1.0, -4.4, 9.8]})" +
            ", (n1:N {a: 2.0, emb: [0.1, 2.0, -3.2, 1.2]})" +
            ", (n2:N {a: 3.0, emb: [0.2, 1.2, -1.1, 1.0]})" +
            ", (n3:N {a: 0.0, emb: [0.1, 0.1, 0.1, 0.1]})" +
            ", (n4:N {a: 1.0, emb: [0.4, 3.3, -0.1, 5.4]})" +
            ", (m0:M {a: 1.0, emb: [-3.2, -4.4, -5.5, -0.1]})" +
            ", (m1:M {a: 2.0, emb: [-4.2, -4.4, -5.5, -0.1]})" +
            ", (m2:M {a: 3.0, emb: [-5.2, -4.4, -5.5, -0.1]})" +
            ", (m3:M {a: 0.0, emb: [-6.2, -4.4, -5.5, -0.1]})" +
            ", (m4:M {a: 1.0, emb: [-7.2, -4.4, -5.5, -0.1]})" +
            ", (p:P {a: 1.0})" +
            ", (n1)-[:T1]->(n2)" +
            ", (n3)-[:T1]->(n4)" +
            ", (n1)-[:T2]->(n3)" +
            ", (n2)-[:T2]->(n4)" +
            ", (m1)-[:T3]->(m2)" +
            ", (m3)-[:T3]->(m4)" +
            ", (m1)-[:T4]->(m3)" +
            ", (m2)-[:T4]->(m4)" +
            ", (m2)-[:T4]->(p)";


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphListProc.class,
            GraphProjectProc.class,
            KGEPredictMutateProc.class
        );

        var query = "CALL gds.graph.project(" +
            "'g', " +
            "{" +
            "  M: { label: 'M', properties: ['a', 'emb'] }, " +
            "  N: { label: 'N', properties: ['a', 'emb'] }, " +
            "  P: { label: 'P', properties: ['a'] }" +
            "}, " +
            "{T1: {type: 'T1', orientation: 'NATURAL'}, T2: {type: 'T2', orientation: 'NATURAL'}}" +
            ")";

        runQuery(query);
    }

    @Test
    void shouldPredictAndMutateKGEModels() {
        var graphStore = GraphStoreCatalog
            .get(getUsername(), DatabaseId.of(db.databaseName()), "g")
            .graphStore();

        var query = GdsCypher
            .call("g")
            .algo("gds.ml.kge.predict")
            .mutateMode()
            .addParameter("mutateRelationshipType", "PREDICTED_T3")
            .addParameter("sourceNodeLabel", "M")
            .addParameter("targetNodeLabel", "N")
            .addParameter("nodeEmbeddingProperty", "emb")
            .addParameter("relationshipTypeEmbedding", List.of(10.5, 12.43, 3.1, 10.0))
            .addParameter("scoringFunction", "TransE")
            .addParameter("threshold", 0.0)  //TODO check
            .addParameter("topK", 2)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "postProcessingMillis", 0L,
            "relationshipsWritten", 10L,
            "configuration", isA(Map.class),
            "probabilityDistribution", allOf(
                hasKey("min"),
                hasKey("max"),
                hasKey("mean"),
                hasKey("stdDev"),
                hasKey("p1"),
                hasKey("p5"),
                hasKey("p10"),
                hasKey("p25"),
                hasKey("p50"),
                hasKey("p75"),
                hasKey("p90"),
                hasKey("p95"),
                hasKey("p99"),
                hasKey("p100")
            ),
            "samplingStats", Map.of(
                "dummyStats", 4.2
            )
        )));

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED_T3"), "similarityScore"));
    }

}
