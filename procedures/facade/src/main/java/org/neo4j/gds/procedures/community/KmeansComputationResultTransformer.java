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

import org.neo4j.gds.algorithms.KmeansSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansMutateResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class KmeansComputationResultTransformer {

    private KmeansComputationResultTransformer() {}

    static Stream<KmeansStreamResult> toStreamResult(StreamComputationResult<KmeansBaseConfig, KmeansResult> computationResult) {
        return computationResult.result().map(kmeansResult -> {
            var communities = kmeansResult.communities();
            var distances = kmeansResult.distanceFromCenter();
            var silhuette = kmeansResult.silhouette();
            var graph = computationResult.graph();
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .mapToObj(nodeId -> new KmeansStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    communities.get(nodeId),
                    distances.get(nodeId),
                    silhuette == null ? -1 : silhuette.get(nodeId)
                ));

        }).orElseGet(Stream::empty);
    }

    static KmeansMutateResult toMutateResult(NodePropertyMutateResult<KmeansSpecificFields> computationResult) {
        return new KmeansMutateResult(
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.nodePropertiesWritten(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.algorithmSpecificFields().centroids(),
            computationResult.algorithmSpecificFields().averageDistanceToCentroid(),
            computationResult.algorithmSpecificFields().averageSilhouette(),
            computationResult.configuration().toMap()
        );
    }


}
