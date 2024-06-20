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

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.algorithms.community.specificfields.AlphaSccSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.procedures.community.scc.AlphaSccWriteResult;
import org.neo4j.gds.procedures.community.scc.SccStatsResult;
import org.neo4j.gds.procedures.community.scc.SccStreamResult;
import org.neo4j.gds.procedures.community.scc.SccWriteResult;
import org.neo4j.gds.scc.SccAlphaWriteConfig;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.scc.SccStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class SccComputationResultTransformer {

    private SccComputationResultTransformer() {}

    static Stream<SccStreamResult> toStreamResult(
        StreamComputationResult<HugeLongArray> computationResult,
        SccStreamConfig configuration
    ) {
        return computationResult.result().map(sccResult -> {
            var graph = computationResult.graph();
            var nodePropertyValues = CommunityCompanion.nodePropertyValues(
                configuration.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(sccResult),
                Optional.empty(),
                configuration.concurrency()
            );
            return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(i -> new SccStreamResult(
                    graph.toOriginalNodeId(i),
                    nodePropertyValues.longValue(i)
                ));

        }).orElseGet(Stream::empty);
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

    static SccWriteResult toWriteResult(NodePropertyWriteResult<StandardCommunityStatisticsSpecificFields> computationResult) {
        return new SccWriteResult(
            computationResult.algorithmSpecificFields().communityCount(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.writeMillis(),
            computationResult.nodePropertiesWritten(),
            computationResult.configuration().toMap()
        );
    }

    static AlphaSccWriteResult toAlphaWriteResult(
        NodePropertyWriteResult<AlphaSccSpecificFields> computationResult,
        SccAlphaWriteConfig config
    ) {
        return new AlphaSccWriteResult(
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.writeMillis(),
            computationResult.algorithmSpecificFields().nodes(),
            computationResult.algorithmSpecificFields().communityCount(),
            //p100 is not a thing that actually exists,  max is probably its closest approximation
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("max"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p99"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p95"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p90"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p75"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p50"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p25"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p10"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p5"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("p1"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("min"),
            (long) computationResult.algorithmSpecificFields().communityDistribution().get("max"),
            config.writeProperty()
        );
    }


}
