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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.conductance.Conductance;
import org.neo4j.gds.impl.conductance.ConductanceStreamConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.conductance.ConductanceProc.CONDUCTANCE_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.conductance.stream", description = CONDUCTANCE_DESCRIPTION, executionMode = STREAM)
public class ConductanceStreamProc extends StreamProc<Conductance, Conductance.Result, ConductanceStreamProc.StreamResult, ConductanceStreamConfig> {

    @Procedure(value = "gds.alpha.conductance.stream", mode = READ)
    @Description(CONDUCTANCE_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(
            graphName,
            configuration
        );

        if (result.isGraphEmpty()) {
            return Stream.empty();
        }

        return result.result()
            .streamCommunityResults()
            .map(communityResult -> new StreamResult(communityResult.community(), communityResult.conductance()));
    }

    @Override
    protected ConductanceStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ConductanceStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Conductance, ConductanceStreamConfig> algorithmFactory() {
        return ConductanceProc.algorithmFactory();
    }

    @Override
    protected StreamResult streamResult(long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues) {
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
