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
package org.neo4j.gds.procedures.algorithms.community.stubs;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.procedures.algorithms.community.CommunityStatisticsWithTimingComputer;
import org.neo4j.gds.procedures.algorithms.community.LeidenMutateResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class LeidenResultBuilderForMutateMode implements ResultBuilder<LeidenMutateConfig, LeidenResult, LeidenMutateResult, Pair<NodePropertiesWritten, NodePropertyValues>> {
    private final CommunityStatisticsWithTimingComputer communityStatisticsWithTimingComputer = new CommunityStatisticsWithTimingComputer();

    private final StatisticsComputationInstructions statisticsComputationInstructions;

    public LeidenResultBuilderForMutateMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public LeidenMutateResult build(
        Graph graph,
        GraphStore graphStore,
        LeidenMutateConfig configuration,
        Optional<LeidenResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<NodePropertiesWritten, NodePropertyValues>> metadata
    ) {
        if (result.isEmpty()) return LeidenMutateResult.emptyFrom(timings, configuration.toMap());

        var leidenResult = result.get();
        var nodePropertiesWrittenAndConvertedNodePropertyValues = metadata.orElseThrow();

        var communityStatisticsWithTiming = communityStatisticsWithTimingComputer.compute(
            configuration,
            statisticsComputationInstructions,
            nodePropertiesWrittenAndConvertedNodePropertyValues.getRight().nodeCount(),
            leidenResult.communities()::get
        );

        return new LeidenMutateResult(
            leidenResult.ranLevels(),
            leidenResult.didConverge(),
            leidenResult.communities().size(),
            communityStatisticsWithTiming.getLeft(),
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatisticsWithTiming.getRight(),
            timings.mutateOrWriteMillis,
            nodePropertiesWrittenAndConvertedNodePropertyValues.getLeft().value(),
            communityStatisticsWithTiming.getMiddle(),
            Arrays.stream(leidenResult.modularities()).boxed().collect(Collectors.toList()),
            leidenResult.modularity(),
            configuration.toMap()
        );
    }
}
