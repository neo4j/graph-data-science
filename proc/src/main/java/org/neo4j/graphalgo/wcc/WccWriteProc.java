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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class WccWriteProc extends WriteProc<Wcc, DisjointSetStruct, WccWriteProc.WriteResult, WccWriteConfig> {

    @Procedure(value = "gds.wcc.write", mode = WRITE)
    @Description(WCC_DESCRIPTION)
    public Stream<WccWriteProc.WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.wcc.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> writeEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected WccWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return WccWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Wcc, WccWriteConfig> algorithmFactory(WccWriteConfig config) {
        return WccProc.algorithmFactory();
    }

    @Override
    protected PropertyTranslator<DisjointSetStruct> nodePropertyTranslator(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult
    ) {
        return WccProc.nodePropertyTranslator(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WccWriteProc.WriteResult> resultBuilder(ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computeResult) {
        return WccProc.resultBuilder(
            new WriteResult.Builder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static final class WriteResult {

        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long componentCount;
        public final Map<String, Object> componentDistribution;
        public final Map<String, Object> configuration;

        WriteResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long componentCount,
            Map<String, Object> componentDistribution,
            Map<String, Object> configuration
        ) {
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
            this.configuration = configuration;
        }

        static class Builder extends AbstractCommunityResultBuilder<WccWriteProc.WriteResult> {

            Builder(
                ProcedureCallContext context,
                AllocationTracker tracker
            ) {
                super(
                    context,
                    tracker
                );
            }

            @Override
            protected WccWriteProc.WriteResult buildResult() {
                return new WccWriteProc.WriteResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingDuration,
                    maybeCommunityCount.orElse(-1L),
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
