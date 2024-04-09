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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.SideEffectProcessingCounts;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class SteinerTreeResultBuilderForStatsMode implements ResultBuilder<SteinerTreeStatsConfig, SteinerTreeResult, Stream<SteinerStatsResult>> {
    @Override
    public Stream<SteinerStatsResult> build(
        Graph graph,
        GraphStore graphStore,
        SteinerTreeStatsConfig configuration,
        Optional<SteinerTreeResult> result,
        AlgorithmProcessingTimings timings,
        SideEffectProcessingCounts counts
    ) {
        var builder = new SteinerStatsResult.Builder();

        builder
            .withConfig(configuration)
            .withComputeMillis(timings.computeMillis)
            .withPreProcessingMillis(timings.preProcessingMillis);

        result.ifPresent(r -> builder
            .withEffectiveNodeCount(r.effectiveNodeCount())
            .withTotalWeight(r.totalCost())
            .withEffectiveTargetNodesCount(r.effectiveTargetNodesCount()));

        return Stream.of(builder.build());
    }
}
