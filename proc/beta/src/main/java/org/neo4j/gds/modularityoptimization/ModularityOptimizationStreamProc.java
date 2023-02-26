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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.modularityOptimization.stream", description = ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION, executionMode = STREAM)
public class ModularityOptimizationStreamProc extends StreamProc<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStreamProc.StreamResult, ModularityOptimizationStreamConfig> {

    @Procedure(name = "gds.beta.modularityOptimization.stream", mode = READ)
    @Description(ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(value = "gds.beta.modularityOptimization.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public GraphAlgorithmFactory<ModularityOptimization, ModularityOptimizationStreamConfig> algorithmFactory() {
        return new ModularityOptimizationFactory<>();
    }

    @Override
    protected StreamResult streamResult(long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues) {
        return new StreamResult(originalNodeId, nodePropertyValues.longValue(internalNodeId));
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStreamConfig> computationResult
    ) {
        return CommunityProcCompanion.nodeProperties(
                computationResult.config(),
                computationResult.result().asNodeProperties()
        );
    }

    @Override
    protected ModularityOptimizationStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ModularityOptimizationStreamConfig.of(config);
    }

    @SuppressWarnings("unused")
    public static class StreamResult {
        public final long nodeId;
        public final long communityId;

        public StreamResult(long nodeId, long communityId) {
            this.nodeId = nodeId;
            this.communityId = communityId;
        }
    }
}
