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
package org.neo4j.gds.wcc;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.wcc.write", description = WCC_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class WccWriteProc extends WriteProc<Wcc, DisjointSetStruct, WccWriteProc.WriteResult, WccWriteConfig> {

    @Procedure(value = "gds.wcc.write", mode = WRITE)
    @Description(WCC_DESCRIPTION)
    public Stream<WccWriteProc.WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult = compute(
            graphName,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.wcc.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected WccWriteConfig newConfig(String username, CypherMapWrapper config) {
        return WccWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Wcc, WccWriteConfig> algorithmFactory() {
        return WccProc.algorithmFactory();
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult
    ) {
        return WccProc.nodeProperties(computationResult, computationResult.config().writeProperty());
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return WccProc.resultBuilder(
            new WriteResult.Builder(executionContext.callContext(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends WccStatsProc.StatsResult {

        public final long writeMillis;
        public final long nodePropertiesWritten;

        WriteResult(
            long componentCount,
            Map<String, Object> componentDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                componentCount,
                componentDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractCommunityResultBuilder<WriteResult> {

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
                    computeMillis,
                    postProcessingDuration,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
