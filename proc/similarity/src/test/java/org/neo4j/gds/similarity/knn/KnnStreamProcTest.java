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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        String query = "CALL gds.beta.knn.stream($graph, {nodeWeightProperty: 'knn', topK: 1})" +
                       " YIELD node1, node2, similarity" +
                       " RETURN node1, node2, similarity" +
                       " ORDER BY node1";

        assertCypherResult(query, Map.of("graph", GRAPH_NAME), List.of(
            Map.of("node1", 0L, "node2", 1L, "similarity", 0.5),
            Map.of("node1", 1L, "node2", 0L, "similarity", 0.5),
            Map.of("node1", 2L, "node2", 1L, "similarity", 0.25)
        ));
    }
}
