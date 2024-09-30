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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Optional;
import java.util.stream.Stream;

class FilteredNodeSimilarityResultBuilderForStatsMode implements StatsResultBuilder<NodeSimilarityResult, Stream<SimilarityStatsResult>> {
    private final GenericNodeSimilarityResultBuilderForStatsMode genericResultBuilder = new GenericNodeSimilarityResultBuilderForStatsMode();

    private final FilteredNodeSimilarityStatsConfig configuration;
    private final boolean shouldComputeSimilarityDistribution;

    FilteredNodeSimilarityResultBuilderForStatsMode(
        FilteredNodeSimilarityStatsConfig configuration,
        boolean shouldComputeSimilarityDistribution
    ) {
        this.configuration = configuration;
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    @Override
    public Stream<SimilarityStatsResult> build(
        Graph graph,
        Optional<NodeSimilarityResult> result,
        AlgorithmProcessingTimings timings
    ) {
        return genericResultBuilder.build(
            configuration.toMap(),
            result,
            timings,
            shouldComputeSimilarityDistribution
        );
    }
}
