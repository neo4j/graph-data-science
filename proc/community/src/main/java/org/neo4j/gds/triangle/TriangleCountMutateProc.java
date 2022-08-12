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
package org.neo4j.gds.triangle;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.triangle.TriangleCountCompanion.DESCRIPTION;
import static org.neo4j.gds.triangle.TriangleCountCompanion.nodePropertyTranslator;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.triangleCount.mutate", description = DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class TriangleCountMutateProc extends MutatePropertyProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateProc.MutateResult, TriangleCountMutateConfig> {

    @Procedure(value = "gds.triangleCount.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.triangleCount.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected TriangleCountMutateConfig newConfig(String username, CypherMapWrapper config) {
        return TriangleCountMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<IntersectingTriangleCount, TriangleCountMutateConfig> algorithmFactory() {
        return new IntersectingTriangleCountFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig> computationResult
    ) {
        return nodePropertyTranslator(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return TriangleCountCompanion.resultBuilder(new TriangleCountMutateBuilder(), computeResult);
    }

    @SuppressWarnings("unused")
    public static class MutateResult extends TriangleCountStatsProc.StatsResult {

        public long mutateMillis;
        public long nodePropertiesWritten;

        MutateResult(
            long globalTriangleCount,
            long nodeCount,
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                globalTriangleCount,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }
    }

    static class TriangleCountMutateBuilder extends TriangleCountCompanion.TriangleCountResultBuilder<MutateResult> {

        @Override
        public MutateResult build() {
            return new MutateResult(
                globalTriangleCount,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
