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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class KnnStreamProc extends StreamProc<Knn, Knn.Result, SimilarityResult, KnnStreamConfig> {

    @Procedure(value = "gds.beta.knn.stream", mode = READ)
    @Description(KNN_DESCRIPTION)
    public Stream<SimilarityResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Knn, Knn.Result, KnnStreamConfig> result = compute(graphNameOrConfig, configuration);
        Graph graph = result.graph();

        if (result.isGraphEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return result.result()
            .streamSimilarityResult()
            .map(similarityResult -> {
                similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                return similarityResult;
            });
    }

    @Procedure(value = "gds.beta.knn.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected SimilarityResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    protected KnnStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return KnnStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Knn, KnnStreamConfig> algorithmFactory() {
        return new KnnFactory<>();
    }
}
