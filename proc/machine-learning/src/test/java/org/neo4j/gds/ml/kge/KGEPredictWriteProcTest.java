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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KGEPredictWriteProcTest extends BaseProcTest {
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
            KGEPredictWriteProc.class
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
    void shouldWriteUniqueRelationships() {
        String query = "CALL gds.ml.kge.predict.write('g', {" +
            " sourceNodeFilter: 'M'," +
            " targetNodeFilter: 'N'," +
            " nodeEmbeddingProperty: 'emb'," +
            " relationshipTypeEmbedding: [10.5, 12.43, 3.1, 10.0]," +
            " scoringFunction: 'TransE'," +
            " writeProperty: 'scoreWeird'," +
            " writeRelationshipType: 'NEWRELTYPE'," +
            " topK: 2" +
            "})" +
            "YIELD *";

        runQueryWithRowConsumer(
            query,
            res -> {
                assertThat(res.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("writeMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("relationshipsWritten").longValue()).isEqualTo(10L);
            }
        );

        final long relCount = runQuery(
            "MATCH (a)-[:NEWRELTYPE]->(b) RETURN id(a) AS a, id(b) AS b",
            result -> result.stream().count()
        );

        assertEquals(relCount, 10);

        final List<Double> listOfProperties = runQuery(
            "MATCH (a)-[r:NEWRELTYPE]->(b) RETURN r.scoreWeird",
            result -> result.stream().map(hashMap -> (Double) hashMap.get("r.scoreWeird")).collect(Collectors.toList())
        );

        assertThat(listOfProperties.toArray()).satisfiesExactlyInAnyOrder(
            value -> assertThat((Double) value).isCloseTo(10.33, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(9.77, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(9.09, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(9.64, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(8.48, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(9.02, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(7.94, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(8.48, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(7.50, Offset.offset(0.01)),
            value -> assertThat((Double) value).isCloseTo(8.02, Offset.offset(0.01))
        );
    }
}
