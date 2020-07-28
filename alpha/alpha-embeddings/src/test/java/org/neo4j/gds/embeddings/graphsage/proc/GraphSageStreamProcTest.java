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
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.graphalgo.GdsCypher;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSageStreamProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.proc.GraphSageBaseProcTest#configVariations")
    void testStreaming(int embeddingSize, String aggregator, ActivationFunction activationFunction) {

        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.alpha.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("nodePropertyNames", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", aggregator)
            .addParameter("activationFunction", activationFunction)
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("degreeAsProperty", true)
            .yields();

        runQueryWithRowConsumer(query, Map.of("embeddingSize", embeddingSize), row -> {
            Number nodeId = row.getNumber("nodeId");
            assertNotNull(nodeId);

            Object o = row.get("embedding");
            assertTrue(o instanceof List);
            Collection<Double> nodeEmbeddings = (List<Double>) o;
            assertEquals(embeddingSize, nodeEmbeddings.size());
        });
    }
}
