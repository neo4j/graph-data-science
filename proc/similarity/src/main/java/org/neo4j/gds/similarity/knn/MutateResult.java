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

import org.neo4j.gds.similarity.SimilarityMutateResult;
import org.neo4j.gds.similarity.SimilarityProc;

import java.util.Map;

@SuppressWarnings("unused")
public class MutateResult extends SimilarityMutateResult {
    public final long ranIterations;
    public final long nodePairsConsidered;
    public final boolean didConverge;

    public MutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long postProcessingMillis,
        long nodesCompared,
        long relationshipsWritten,
        Map<String, Object> similarityDistribution,
        boolean didConverge,
        long ranIterations,
        long nodePairsConsidered,
        Map<String, Object> configuration
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            mutateMillis,
            postProcessingMillis,
            nodesCompared,
            relationshipsWritten,
            similarityDistribution,
            configuration
        );

        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.nodePairsConsidered = nodePairsConsidered;
    }

    public static class Builder extends SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> {
        private long ranIterations;
        private boolean didConverge;
        private long nodePairsConsidered;

        @Override
        public MutateResult build() {
            return new MutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipsWritten,
                distribution(),
                didConverge,
                ranIterations,
                nodePairsConsidered,
                config.toMap()
            );
        }

        public Builder didConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public Builder ranIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        Builder withNodePairsConsidered(long nodePairsConsidered) {
            this.nodePairsConsidered = nodePairsConsidered;
            return this;
        }
    }
}
