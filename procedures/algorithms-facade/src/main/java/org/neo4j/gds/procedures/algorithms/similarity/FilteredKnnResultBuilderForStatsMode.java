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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class FilteredKnnResultBuilderForStatsMode implements ResultBuilder<FilteredKnnStatsConfig, FilteredKnnResult, Stream<KnnStatsResult>, Void> {
    private final boolean shouldComputeSimilarityDistribution;

    FilteredKnnResultBuilderForStatsMode(boolean shouldComputeSimilarityDistribution) {
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    @Override
    public Stream<KnnStatsResult> build(
        Graph graph,
        GraphStore graphStore,
        FilteredKnnStatsConfig configuration,
        Optional<FilteredKnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return Stream.of(KnnStatsResult.emptyFrom(
            timings,
            configurationMap
        ));

        return new SimilarityStatsProcessor().process(
            graph,
            configuration,
            result.get().similarityResultStream(),
            shouldComputeSimilarityDistribution,
            timings,
            configurationMap,
            result.get().nodesCompared(),
            result.get().numberOfSimilarityPairs(),
            result.get().didConverge(),
            result.get().ranIterations(),
            result.get().nodePairsConsidered()
        );
    }
}
