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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.algorithms.centrality.PageRankDistributionComputer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankWriteConfig;

import java.util.Optional;
import java.util.stream.Stream;

class PageRankResultBuilderForWriteMode implements ResultBuilder<PageRankWriteConfig, PageRankResult, Stream<PageRankWriteResult>, NodePropertiesWritten> {
    private final boolean shouldComputeCentralityDistribution;

    PageRankResultBuilderForWriteMode(boolean shouldComputeCentralityDistribution) {
        this.shouldComputeCentralityDistribution = shouldComputeCentralityDistribution;
    }

    @Override
    public Stream<PageRankWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        PageRankWriteConfig configuration,
        Optional<PageRankResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return Stream.of(PageRankWriteResult.emptyFrom(timings, configurationMap));

        var pageRankResult = result.get();

        var pageRankDistribution = PageRankDistributionComputer.computeDistribution(
            pageRankResult,
            configuration,
            shouldComputeCentralityDistribution
        );

        var pageRankWriteResult = new PageRankWriteResult(
            pageRankResult.iterations(),
            pageRankResult.didConverge(),
            pageRankDistribution.centralitySummary,
            timings.preProcessingMillis,
            timings.computeMillis,
            pageRankDistribution.postProcessingMillis,
            timings.mutateOrWriteMillis,
            metadata.orElseThrow().value(),
            configurationMap
        );

        return Stream.of(pageRankWriteResult);
    }
}
