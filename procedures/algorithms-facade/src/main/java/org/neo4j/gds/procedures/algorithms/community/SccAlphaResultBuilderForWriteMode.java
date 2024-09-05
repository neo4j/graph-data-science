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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccAlphaWriteConfig;

import java.util.Optional;
import java.util.stream.Stream;

class SccAlphaResultBuilderForWriteMode implements ResultBuilder<SccAlphaWriteConfig, HugeLongArray, Stream<AlphaSccWriteResult>, NodePropertiesWritten> {
    private final CommunityStatisticsWithTimingComputer communityStatisticsWithTimingComputer = new CommunityStatisticsWithTimingComputer();

    private final StatisticsComputationInstructions statisticsComputationInstructions;

    SccAlphaResultBuilderForWriteMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<AlphaSccWriteResult> build(
        Graph graph,
        SccAlphaWriteConfig configuration,
        Optional<HugeLongArray> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return Stream.of(AlphaSccWriteResult.emptyFrom(timings, configuration.writeProperty()));

        var hugeLongArray = result.get();

        var nodePropertyValues = NodePropertyValuesAdapter.adapt(hugeLongArray);

        var communityStatisticsWithTiming = communityStatisticsWithTimingComputer.compute(
            configuration,
            statisticsComputationInstructions,
            nodePropertyValues.nodeCount(),
            hugeLongArray::get
        );

        var communityDistribution = communityStatisticsWithTiming.getMiddle();

        var alphaSccWriteResult = new AlphaSccWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatisticsWithTiming.getRight(),
            timings.mutateOrWriteMillis,
            hugeLongArray.size(),
            communityStatisticsWithTiming.getLeft(),
            (long) communityDistribution.get("max"),
            (long) communityDistribution.get("p99"),
            (long) communityDistribution.get("p95"),
            (long) communityDistribution.get("p90"),
            (long) communityDistribution.get("p75"),
            (long) communityDistribution.get("p50"),
            (long) communityDistribution.get("p25"),
            (long) communityDistribution.get("p10"),
            (long) communityDistribution.get("p5"),
            (long) communityDistribution.get("p1"),
            (long) communityDistribution.get("min"),
            (long) communityDistribution.get("max"),
            configuration.writeProperty()
        );

        return Stream.of(alphaSccWriteResult);
    }
}
