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
package org.neo4j.gds.conductance;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.conductance.Conductance;
import org.neo4j.gds.impl.conductance.ConductanceStreamConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.conductance.ConductanceProc.CONDUCTANCE_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ConductanceStreamProc extends StreamProc<Conductance, Conductance.Result, ConductanceStreamProc.StreamResult, ConductanceStreamConfig> {

    @Procedure(value = "gds.alpha.conductance.stream", mode = READ)
    @Description(CONDUCTANCE_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Conductance, Conductance.Result, ConductanceStreamConfig> result = compute(
            graphNameOrConfig,
            configuration
        );

        if (result.isGraphEmpty()) {
            result.graph().release();
            return Stream.empty();
        }

        return result.result()
            .streamCommunityResults()
            .map(communityResult -> new StreamResult(communityResult.community(), communityResult.conductance()));
    }

    @Override
    protected ConductanceStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ConductanceStreamConfig.of(graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Conductance, ConductanceStreamConfig> algorithmFactory() {
        return ConductanceProc.algorithmFactory();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("Conductance handles result building individually.");
    }

    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long community;
        public final double conductance;

        public StreamResult(long community, double conductance) {
            this.community = community;
            this.conductance = conductance;
        }
    }
}
