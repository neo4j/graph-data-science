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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResultBuilder;
import org.neo4j.gds.similarity.SimilarityWriteProc;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.knn.filtered.write", executionMode = WRITE_RELATIONSHIP)
public class FilteredKnnWriteProc extends SimilarityWriteProc<FilteredKnn, FilteredKnnResult, FilteredKnnWriteProcResult, FilteredKnnWriteConfig> {
    @Procedure(name = "gds.alpha.knn.filtered.write", mode = WRITE)
    @Description(FilteredKnnConstants.PROCEDURE_DESCRIPTION)
    public Stream<FilteredKnnWriteProcResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Override
    public String procedureName() {
        return "Filtered KNN";
    }

    @Override
    protected SimilarityResultBuilder<FilteredKnnWriteProcResult> resultBuilder(ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return new FilteredKnnWriteProcResult.Builder();
        }

        return new FilteredKnnWriteProcResult.Builder()
            .withDidConverge(computationResult.result().didConverge())
            .withNodePairsConsidered(computationResult.result().nodePairsConsidered())
            .withRanIterations(computationResult.result().ranIterations());
    }

    @Override
    protected FilteredKnnWriteConfig newConfig(String username, CypherMapWrapper config) {
        return FilteredKnnWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FilteredKnn, FilteredKnnWriteConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    protected SimilarityGraphResult similarityGraphResult(ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig> computationResult) {
        FilteredKnn algorithm = Objects.requireNonNull(computationResult.algorithm());
        FilteredKnnWriteConfig config = computationResult.config();
        return FilteredKnnHelpers.computeToGraph(
            computationResult.graph(),
            algorithm.nodeCount(),
            config.concurrency(),
            Objects.requireNonNull(computationResult.result()),
            algorithm.executorService()
        );
    }
}
