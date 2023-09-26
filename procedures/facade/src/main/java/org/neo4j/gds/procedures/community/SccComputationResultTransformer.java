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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.procedures.community.scc.SccMutateResult;
import org.neo4j.gds.procedures.community.scc.SccStatsResult;
import org.neo4j.gds.procedures.community.scc.SccStreamResult;
import org.neo4j.gds.scc.SccStatsConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.scc.Scc.UNORDERED;

final class SccComputationResultTransformer {

    private SccComputationResultTransformer() {}

    static Stream<SccStreamResult> toStreamResult(StreamComputationResult<HugeLongArray> computationResult) {
        return computationResult.result().map(wccResult -> {
            var graph = computationResult.graph();
            var components = computationResult.result().orElseGet(() -> HugeLongArray.newArray(0));
            return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(i -> components.get(i) != UNORDERED)
                .mapToObj(i -> new SccStreamResult(graph.toOriginalNodeId(i), components.get(i)));

        }).orElseGet(Stream::empty);
    }

    static SccMutateResult toMutateResult(NodePropertyMutateResult<StandardCommunityStatisticsSpecificFields> computationResult) {
        return new SccMutateResult(
            computationResult.algorithmSpecificFields().communityCount(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.nodePropertiesWritten(),
            computationResult.configuration().toMap()
        );
    }

    static SccStatsResult toStatsResult(
        StatsResult<StandardCommunityStatisticsSpecificFields> computationResult,
        SccStatsConfig config
    ) {
        return new SccStatsResult(
            computationResult.algorithmSpecificFields().communityCount(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            config.toMap()
        );
    }

}
