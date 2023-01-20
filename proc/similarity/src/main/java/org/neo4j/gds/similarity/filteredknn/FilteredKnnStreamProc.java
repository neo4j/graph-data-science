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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.knn.filtered.stream", executionMode = ExecutionMode.STREAM)
public class FilteredKnnStreamProc extends StreamProc<FilteredKnn, FilteredKnnResult, SimilarityResult, FilteredKnnStreamConfig> {
    @Procedure(value = "gds.alpha.knn.filtered.stream", mode = READ)
    @Description(FilteredKnnConstants.PROCEDURE_DESCRIPTION)
    public Stream<SimilarityResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected SimilarityResult streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException("Filtered KNN handles result building individually.");
    }

    @Override
    protected FilteredKnnStreamConfig newConfig(String username, CypherMapWrapper config) {
        return FilteredKnnStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FilteredKnn, FilteredKnnStreamConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnStreamConfig, Stream<SimilarityResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();

            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }

            return computationResult.result()
                .similarityResultStream()
                .map(similarityResult -> {
                    similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                    similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                    return similarityResult;
                });
        };
    }
}
