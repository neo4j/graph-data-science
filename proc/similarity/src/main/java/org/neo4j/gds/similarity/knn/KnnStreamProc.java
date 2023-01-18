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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.knn.stream", description = KNN_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class KnnStreamProc extends StreamProc<Knn, Knn.Result, SimilarityResult, KnnStreamConfig> {

    @Procedure(value = "gds.knn.stream", mode = READ)
    @Description(KNN_DESCRIPTION)
    public Stream<SimilarityResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(value = "gds.knn.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected SimilarityResult streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    protected KnnStreamConfig newConfig(String username, CypherMapWrapper config) {
        return KnnStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Knn, KnnStreamConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    public ComputationResultConsumer<Knn, Knn.Result, KnnStreamConfig, Stream<SimilarityResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();

            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }

            return computationResult.result()
                .streamSimilarityResult()
                .map(similarityResult -> {
                    similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                    similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                    return similarityResult;
                });
        };
    }
}
