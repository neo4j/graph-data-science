/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.ocd;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.ocd.CommunityAffiliations;
import org.neo4j.graphalgo.impl.ocd.OverlappingCommunityDetection;
import org.neo4j.graphalgo.impl.ocd.OverlappingCommunityDetectionStreamConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class OverlappingCommunityDetectionStreamProc extends OverlappingCommunityDetectionBaseProc<OverlappingCommunityDetectionStreamConfig>{
    @Procedure(name = "gds.alpha.ocd.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<OverlappingCommunityDetection, CommunityAffiliations, OverlappingCommunityDetectionStreamConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        CommunityAffiliations affiliations = computationResult.result();
        double delta = affiliations.getDelta();
        Graph graph = computationResult.graph();
        long nodeCount = graph.nodeCount();
        return IntStream.range(0, (int)nodeCount).mapToObj(nodeId -> {
            List<Integer> exceeding = affiliations.nodeAffiliations(nodeId).exceeding(delta);
            List<Double> exceedingScores = affiliations.nodeAffiliations(nodeId).exceedingScores(delta);
            List<Long> communityIds = exceeding.stream().map(graph::toOriginalNodeId).collect(Collectors.toList());
            return new StreamResult(nodeId, communityIds, exceedingScores);
        });
    }

    @Override
    protected OverlappingCommunityDetectionStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return OverlappingCommunityDetectionStreamConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    public static final class StreamResult {
        public final long nodeId;
        public final List<Long> communityIds;
        public final List<Double> scores;

        StreamResult(long nodeId, List<Long> communityIds, List<Double> scores) {
            this.nodeId = nodeId;
            this.communityIds = communityIds;
            this.scores = scores;
        }
    }
}
