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
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

public class KGEPredictStreamProcTest extends BaseProcTest {

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

    @Inject
    IdFunction idFunction;


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphListProc.class,
            GraphProjectProc.class,
            KGEPredictStreamProc.class
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
    void shouldPredictAndStreamKGEResults() {
        var n0 = idFunction.of("n0");
        var n4 = idFunction.of("n4");
        var m0 = idFunction.of("m0");
        var m1 = idFunction.of("m1");
        var m2 = idFunction.of("m2");
        var m3 = idFunction.of("m3");
        var m4 = idFunction.of("m4");

        assertCypherResult(
            "CALL gds.ml.kge.predict.stream('g', {" +
                " sourceNodeFilter: 'M'," +
                " targetNodeFilter: 'N'," +
                " nodeEmbeddingProperty: 'emb'," +
                " relationshipTypeEmbedding: [10.5, 12.43, 3.1, 10.0]," +
                " scoringFunction: 'TransE'," +
                " topK: 2" +
                "})" +
                "YIELD sourceNodeId, targetNodeId, score" +
                " RETURN sourceNodeId, targetNodeId, score" +
                " ORDER BY sourceNodeId",
            List.of(
                Map.of("sourceNodeId", m0, "targetNodeId", n4, "score", 9.77358173854396),
                Map.of("sourceNodeId", m0, "targetNodeId", n0, "score", 10.33058081619809),
                Map.of("sourceNodeId", m1, "targetNodeId", n4, "score", 9.09521302664209),
                Map.of("sourceNodeId", m1, "targetNodeId", n0, "score", 9.649917098089496),
                Map.of("sourceNodeId", m2, "targetNodeId", n4, "score", 8.480736996275736),
                Map.of("sourceNodeId", m2, "targetNodeId", n0, "score", 9.028892512373819),
                Map.of("sourceNodeId", m3, "targetNodeId", n4, "score", 7.944992133413349),
                Map.of("sourceNodeId", m3, "targetNodeId", n0, "score", 8.480619081175618),
                Map.of("sourceNodeId", m4, "targetNodeId", n4, "score", 7.50485842637954),
                Map.of("sourceNodeId", m4, "targetNodeId", n0, "score", 8.020031172009245)
                )
        );

    }

}
