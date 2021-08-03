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
package org.neo4j.graphalgo.similarity.knn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KnnStreamProcTest extends KnnProcTest<KnnStreamConfig> {

    private static final Collection<SimilarityResult> EXPECTED = new HashSet<>();
    static {
        EXPECTED.add(new SimilarityResult(0, 1, 0.5));
        EXPECTED.add(new SimilarityResult(1, 0, 0.5));
        EXPECTED.add(new SimilarityResult(2, 1, 0.25));
    }

    @Override
    public Class<? extends AlgoBaseProc<Knn, Knn.Result, KnnStreamConfig>> getProcedureClazz() {
        return KnnStreamProc.class;
    }

    @Override
    public KnnStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return KnnStreamConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void shouldStreamResults() {
        String query = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("gds.beta.knn")
            .streamMode()
            .addParameter("sudo", true)
            .addParameter("nodeWeightProperty", "knn")
            .addParameter("topK", 1)
            .yields();

        Collection<SimilarityResult> result = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            long node1 = row.getNumber("node1").longValue();
            long node2 = row.getNumber("node2").longValue();
            double similarity = row.getNumber("similarity").doubleValue();
            result.add(new SimilarityResult(node1, node2, similarity));
        });

        assertEquals(EXPECTED, result);
    }
}
