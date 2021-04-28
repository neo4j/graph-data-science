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
package org.neo4j.graphalgo.triangle;


import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.config.GraphCreateConfigValidations.validateIsUndirectedGraph;
import static org.neo4j.graphalgo.triangle.LocalClusteringCoefficientCompanion.warnOnGraphWithParallelRelationships;
import static org.neo4j.procedure.Mode.READ;

public class LocalClusteringCoefficientStreamProc
    extends StreamProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result,
    LocalClusteringCoefficientStreamProc.Result, LocalClusteringCoefficientStreamConfig> {

    @Description("")
    @Procedure(name = "gds.localClusteringCoefficient.stream", mode = READ)
    public Stream<Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.localClusteringCoefficient.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected LocalClusteringCoefficientStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LocalClusteringCoefficientStreamConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<LocalClusteringCoefficient, LocalClusteringCoefficientStreamConfig> algorithmFactory() {
        return new LocalClusteringCoefficientFactory<>();
    }

    @Override
    protected Result streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new Result(originalNodeId, nodeProperties.doubleValue(internalNodeId));
    }

    @Override
    protected void validateConfigsBeforeLoad(
        GraphCreateConfig graphCreateConfig, LocalClusteringCoefficientStreamConfig config
    ) {
        validateIsUndirectedGraph(graphCreateConfig, config);
        warnOnGraphWithParallelRelationships(graphCreateConfig, config, log);
    }

    @Override
    protected NodeProperties nodeProperties(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStreamConfig> computationResult
    ) {
        return LocalClusteringCoefficientCompanion.nodeProperties(computationResult);
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final long nodeId;
        public final double localClusteringCoefficient;

        public Result(long nodeId, double localClusteringCoefficient) {
            this.nodeId = nodeId;
            this.localClusteringCoefficient = localClusteringCoefficient;
        }
    }
}

