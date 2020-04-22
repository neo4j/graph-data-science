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
package org.neo4j.graphalgo.triangle;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.triangle.TriangleCountCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class TriangleCountWriteProc extends WriteProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountWriteProc.WriteResult, TriangleCountWriteConfig> {

    @Procedure(value = "gds.triangleCount.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.triangleCount.write.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected void validateConfigs(
        GraphCreateConfig graphCreateConfig, TriangleCountWriteConfig config
    ) {
        TriangleCountCompanion.validateConfigs(graphCreateConfig, config);
    }

    @Override
    protected TriangleCountWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleCountWriteConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<IntersectingTriangleCount, TriangleCountWriteConfig> algorithmFactory(
        TriangleCountWriteConfig config
    ) {
        return new IntersectingTriangleCountFactory<>();
    }

    @Override
    protected PropertyTranslator<IntersectingTriangleCount.TriangleCountResult> nodePropertyTranslator(ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountWriteConfig> computationResult) {
        return TriangleCountCompanion.nodePropertyTranslator();
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountWriteConfig> computeResult) {
        return TriangleCountCompanion.resultBuilder(
            new TriangleCountWriteBuilder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    @Override
    protected Collection<NodePropertyExporter.NodeProperty<?>> nodePropertiesToWrite(ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountWriteConfig> computationResult) {
        TriangleCountWriteConfig config = computationResult.config();
        IntersectingTriangleCount.TriangleCountResult result = computationResult.result();

        return List.of(
            ImmutableNodeProperty.of(
                config.writeProperty(),
                result.localTriangles(),
                HugeAtomicLongArray.Translator.INSTANCE
            ),
            ImmutableNodeProperty.of(
                config.clusteringCoefficientProperty(),
                result.localClusteringCoefficients(),
                HugeDoubleArray.Translator.INSTANCE
            )
        );
    }

    public static class WriteResult extends TriangleCountStatsProc.StatsResult {

        public long writeMillis;
        public long nodePropertiesWritten;

        public WriteResult(
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodeCount,
            long triangleCount,
            double averageClusteringCoefficient,
            long nodePropertiesWritten,
            @Nullable Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            super(
                createMillis,
                computeMillis,
                postProcessingMillis,
                nodeCount,
                triangleCount,
                averageClusteringCoefficient,
                communityDistribution,
                configuration
            );

            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }
    }

    static class TriangleCountWriteBuilder extends TriangleCountCompanion.TriangleCountResultBuilder<WriteResult> {

        TriangleCountWriteBuilder(
            ProcedureCallContext callContext,
            AllocationTracker tracker
        ) {
            super(callContext, tracker);
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                nodeCount,
                triangleCount,
                averageClusteringCoefficient,
                nodePropertiesWritten,
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }
}
