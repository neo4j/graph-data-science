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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.config.ConcurrencyConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class BetweennessCentralityResultBuilderForWriteMode implements ResultBuilder<BetweennessCentralityWriteConfig, CentralityAlgorithmResult, Stream<CentralityWriteResult>, NodePropertiesWritten> {
    private final CentralityDistributionComputer centralityDistribution = new CentralityDistributionComputer();
    private final boolean shouldComputeCentralityDistribution;

    BetweennessCentralityResultBuilderForWriteMode(boolean shouldComputeCentralityDistribution) {
        this.shouldComputeCentralityDistribution = shouldComputeCentralityDistribution;
    }

    @Override
    public Stream<CentralityWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        BetweennessCentralityWriteConfig configuration,
        Optional<CentralityAlgorithmResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return Stream.of(CentralityWriteResult.emptyFrom(
            timings,
            configurationMap
        ));

        var centralityDistributionAndTiming = computeCentralityDistribution(
            graph,
            configuration,
            result.get()
        );

        return Stream.of(
            new CentralityWriteResult(
                metadata.orElseThrow().value,
                timings.preProcessingMillis,
                timings.computeMillis,
                centralityDistributionAndTiming.getRight(),
                timings.mutateOrWriteMillis,
                centralityDistributionAndTiming.getLeft(),
                configurationMap
            )
        );
    }

    private Pair<Map<String, Object>, Long> computeCentralityDistribution(
        IdMap graph,
        ConcurrencyConfig configuration,
        CentralityAlgorithmResult result
    ) {
        return centralityDistribution.compute(
            graph,
            result.centralityScoreProvider(),
            configuration,
            shouldComputeCentralityDistribution
        );
    }
}
