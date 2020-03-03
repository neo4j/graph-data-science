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

import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.ocd.LocallyMinimalNeighborhoods;
import org.neo4j.graphalgo.impl.ocd.LocallyMinimalNeighborhoodsStreamConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LocallyMinimalNeighborhoodsStreamProc extends LocallyMinimalNeighborhoodsBaseProc<LocallyMinimalNeighborhoodsStreamConfig> {
    @Procedure(value = "gds.alpha.lmn.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result, LocallyMinimalNeighborhoodsStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stream(computationResult);
    }
    @Override
    protected LocallyMinimalNeighborhoodsStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LocallyMinimalNeighborhoodsStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    private Stream<StreamResult> stream(ComputationResult<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result, LocallyMinimalNeighborhoodsStreamConfig> computationResult) {
        if (computationResult.config().includeMembers()) {
            return streamMembers(computationResult);
        }
        else {
            return streamCentersOnly(computationResult);
        }
    }

    private Stream<StreamResult> streamCentersOnly(ComputationResult<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result, LocallyMinimalNeighborhoodsStreamConfig> computationResult) {
        LocallyMinimalNeighborhoods.Result result = computationResult.result();
        return Iterables.stream(result.neighborhoodCenters).map(center ->
            new StreamResult(
                center,
                center,
                result.conductances.get(center)
            )
        );
    }

    private Stream<StreamResult> streamMembers(ComputationResult<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result, LocallyMinimalNeighborhoodsStreamConfig> computationResult) {
        LocallyMinimalNeighborhoods.Result result = computationResult.result();
        return Iterables.stream(result.neighborhoodCenters).flatMap(center -> {
            LongSet members = result.communityMemberships.get(center);
            return Iterables.stream(members).map(member ->
                new StreamResult(
                    member.value,
                    center,
                    result.conductances.get(member.value)
                )
            );
        });
    }

    public static final class StreamResult {
        public final long nodeId;
        public final long communityId;
        public final double conductance;

        StreamResult(long nodeId, long communityId, double conductance) {
            this.nodeId = nodeId;
            this.communityId = communityId;
            this.conductance = conductance;
        }
    }
}
