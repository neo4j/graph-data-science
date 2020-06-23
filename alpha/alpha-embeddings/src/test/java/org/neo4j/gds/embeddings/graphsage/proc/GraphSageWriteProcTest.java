/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.GdsCypher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class GraphSageWriteProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("configVariations")
    void testWriting(int embeddingSize, String aggregator, String activationFunction) {

        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.alpha.graphSage")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("nodePropertyNames", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", aggregator)
            .addParameter("activationFunction", activationFunction)
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("sampleSizes", List.of(25, 10))
            .addParameter("degreeAsProperty", true)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotNull(row.get("startLoss"));
            assertNotNull(row.get("epochLosses"));
            assertNotNull(row.get("nodeCount"));
            assertNotNull(row.get("nodePropertiesWritten"));
            assertNotNull(row.get("createMillis"));
            assertNotNull(row.get("computeMillis"));
            assertNotNull(row.get("writeMillis"));
            assertNotNull(row.get("configuration"));
        });

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            results.add(Map.of("size", (long)embeddingSize));
        }

        assertCypherResult(
            "MATCH (n:King) RETURN size(n.embedding) AS size",
            results
        );
    }
}
