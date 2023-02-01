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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.k1coloring.K1ColoringProc.K1_COLORING_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.beta.k1coloring.write", description = K1ColoringProc.K1_COLORING_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class K1ColoringWriteProc extends WriteProc<K1Coloring, HugeLongArray, K1ColoringWriteProc.WriteResult, K1ColoringWriteConfig> {

    @Procedure(name = "gds.beta.k1coloring.write", mode = WRITE)
    @Description(K1_COLORING_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computationResult =
            compute(graphName, configuration);

        return write(computationResult);
    }

    @Procedure(value = "gds.beta.k1coloring.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computationResult) {
        return K1ColoringProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        WriteResult.Builder builder = new WriteResult.Builder(executionContext.callContext(), computeResult.config().concurrency());
        return K1ColoringProc.resultBuilder(builder, computeResult, executionContext.callContext());
    }

    @Override
    public GraphAlgorithmFactory<K1Coloring, K1ColoringWriteConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    protected K1ColoringWriteConfig newConfig(String username, CypherMapWrapper config) {
        return K1ColoringWriteConfig.of(config);
    }

    @SuppressWarnings("unused")
    public static class WriteResult {

        public static final WriteResult EMPTY = new WriteResult(
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            null
        );

        public final long preProcessingMillis;
        public final long computeMillis;
        public final long writeMillis;

        public final long nodeCount;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;

        public Map<String, Object> configuration;

        WriteResult(
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long nodeCount,
            long colorCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.colorCount = colorCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        static class Builder extends K1ColoringProc.K1ColoringResultBuilder<WriteResult> {

            Builder(
                ProcedureCallContext context,
                int concurrency
            ) {
                super(context, concurrency);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    nodeCount,
                    colorCount,
                    ranIterations,
                    didConverge,
                    config.toMap()
                );
            }
        }
    }

}
