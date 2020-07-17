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
package org.neo4j.graphalgo.pregel.cc;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.PregelResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

// generate
public class ConnectectedComponentsStreamProc extends StreamProc<
    ConnectedComponentsAlgorithm,
    HugeDoubleArray,
    PregelResult,
    ConnectedComponentsConfig> {

    @Procedure(value = "gds.pregel.cc.stream", mode = Mode.READ)
    @Description("Computed connected components")
    public Stream<PregelResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected PregelResult streamResult(long originalNodeId, double value) {
        return new PregelResult(originalNodeId, value);
    }

    @Override
    protected ConnectedComponentsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ConnectedComponentsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ConnectedComponentsAlgorithm, ConnectedComponentsConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public ConnectedComponentsAlgorithm build(
                Graph graph,
                ConnectedComponentsConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ConnectedComponentsAlgorithm(graph, configuration, tracker, log);
            }

            @Override
            public MemoryEstimation memoryEstimation(ConnectedComponentsConfig configuration) {
                return MemoryEstimations.empty();
            }
        };
    }
}
