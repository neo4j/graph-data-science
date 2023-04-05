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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResultBuilder;
import org.neo4j.gds.similarity.SimilarityWriteResult;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class FilteredKnnHelpers {
    private FilteredKnnHelpers() {}

    static SimilarityGraphResult computeToGraph(
        Graph graph,
        long nodeCount,
        int concurrency,
        FilteredKnnResult result,
        ExecutorService executor
    ) {
        Graph similarityGraph = new SimilarityGraphBuilder(
            graph,
            concurrency,
            executor
        ).build(result.similarityResultStream());
        return new SimilarityGraphResult(similarityGraph, nodeCount, false);
    }

    @SuppressWarnings("unused")
    public static class Result extends SimilarityWriteResult {
        public final long ranIterations;
        public final boolean didConverge;
        public final long nodePairsConsidered;

        Result(
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            boolean didConverge,
            long ranIterations,
            long nodePairsCompared,
            Map<String, Object> similarityDistribution,
            Map<String, Object> configuration
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipsWritten,
                similarityDistribution,
                configuration
            );

            this.nodePairsConsidered = nodePairsCompared;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
        }

        @SuppressWarnings("unused")
        static class Builder extends SimilarityResultBuilder<Result> {
            public long ranIterations;
            public boolean didConverge;
            public long nodePairsConsidered;

            @Override
            public Result build() {
                return new Result(
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingMillis,
                    nodesCompared,
                    relationshipsWritten,
                    didConverge,
                    ranIterations,
                    nodePairsConsidered,
                    distribution(),
                    config.toMap()
                );
            }

            public Builder withDidConverge(boolean didConverge) {
                this.didConverge = didConverge;
                return this;
            }

            public Builder withRanIterations(long ranIterations) {
                this.ranIterations = ranIterations;
                return this;
            }

            Builder withNodePairsConsidered(long nodePairsConsidered) {
                this.nodePairsConsidered = nodePairsConsidered;
                return this;
            }
        }
    }
}
