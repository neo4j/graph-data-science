/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.impl.wcc.Wcc;
import org.neo4j.graphalgo.impl.wcc.WccWriteConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class WccWriteProc extends WccBaseProc<WccWriteConfig> {

    @Procedure(value = "gds.algo.wcc.write", mode = WRITE)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, true);
    }

    @Procedure(value = "gds.algo.wcc.stats", mode = READ)
    public Stream<WriteResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, false);
    }

    @Procedure(value = "gds.algo.wcc.write.estimate", mode = READ)
    public Stream<MemoryEstimateResult> writeEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.algo.wcc.stats.estimate", mode = READ)
    public Stream<MemoryEstimateResult> statsEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
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
    protected Optional<PropertyTranslator<DisjointSetStruct>> nodePropertyTranslator(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult
    ) {
        WccWriteConfig config = computationResult.config();

        boolean consecutiveIds = config.consecutiveIds();
        boolean isIncremental = config.isIncremental();
        boolean seedPropertyEqualsWriteProperty = config.writeProperty().equalsIgnoreCase(config.seedProperty());

        PropertyTranslator<DisjointSetStruct> propertyTranslator;
        if (seedPropertyEqualsWriteProperty && !consecutiveIds) {
            NodeProperties seedProperties = computationResult.graph().nodeProperties(config.seedProperty());
            propertyTranslator = new PropertyTranslator.OfLongIfChanged<>(seedProperties, DisjointSetStruct::setIdOf);
        } else if (consecutiveIds && !isIncremental) {
            propertyTranslator = new ConsecutivePropertyTranslator(computationResult.result(), computationResult.tracker());
        } else {
            propertyTranslator = (PropertyTranslator.OfLong<DisjointSetStruct>) DisjointSetStruct::setIdOf;
        }

        return Optional.of(propertyTranslator);
    }

    private Stream<WccWriteProc.WriteResult> write(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computeResult,
        boolean write
    ) {
        if (computeResult.isGraphEmpty()) {
            return Stream.of(WriteResult.empty(computeResult.config(), computeResult.createMillis()));
        } else {
            WccWriteConfig config = computeResult.config();
            Graph graph = computeResult.graph();

            WriteResultBuilder builder = new WriteResultBuilder(config, callContext, computeResult.tracker());
            DisjointSetStruct dss = computeResult.result();

            builder.setCreateMillis(computeResult.createMillis());
            builder.setComputeMillis(computeResult.computeMillis());
            builder.withCommunityFunction(dss::setIdOf);

            if (write && !config.writeProperty().isEmpty()) {
                writeNodeProperties(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    public static final class WriteResult {

        public final String writeProperty;
        public final String seedProperty;
        public final String weightProperty;
        public final long nodePropertiesWritten;
        public final long relationshipPropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long componentCount;
        public final double threshold;
        public final boolean consecutiveIds;
        public final Map<String, Object> componentDistribution;

        static WriteResult empty(WccWriteConfig config, long createMillis) {
            return new WriteResult(
                config,
                0,
                createMillis,
                0, 0, 0, 0,
                Collections.emptyMap()
            );
        }

        WriteResult(
            WccWriteConfig config,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long componentCount,
            Map<String, Object> componentDistribution
        ) {
            this.writeProperty = config.writeProperty();
            this.seedProperty = config.seedProperty();
            this.weightProperty = config.weightProperty();
            this.threshold = config.threshold();
            this.consecutiveIds = config.consecutiveIds();

            this.nodePropertiesWritten = nodePropertiesWritten;
            this.relationshipPropertiesWritten = 0L;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
        }
    }

    static class WriteResultBuilder extends AbstractCommunityResultBuilder<WccWriteConfig, WriteResult> {

        private final WccWriteConfig config;

        WriteResultBuilder(WccWriteConfig config, ProcedureCallContext context, AllocationTracker tracker) {
            super(
                config,
                context,
                tracker
            );
            this.config = config;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                config,
                nodePropertiesWritten,  // should be nodePropertiesWritten
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                maybeCommunityCount.orElse(-1L),
                communityHistogramOrNull()
            );
        }
    }
}
