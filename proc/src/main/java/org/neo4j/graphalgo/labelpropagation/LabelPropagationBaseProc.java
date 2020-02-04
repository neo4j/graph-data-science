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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;
import java.util.stream.Stream;

public abstract class LabelPropagationBaseProc<CONFIG extends LabelPropagationBaseConfig> extends AlgoBaseProc<LabelPropagation, LabelPropagation, CONFIG> {

    static final String LABEL_PROPAGATION_DESCRIPTION =
        "The Label Propagation algorithm is a fast algorithm for finding communities in a graph.";

    @Override
    protected LabelPropagationFactory<CONFIG> algorithmFactory(LabelPropagationBaseConfig config) {
        return new LabelPropagationFactory<>(config);
    }

    protected Stream<WriteResult> write(
        ComputationResult<LabelPropagation, LabelPropagation, CONFIG> computationResult
    ) {
        log.debug("Writing results");

        CONFIG config = computationResult.config();

        Graph graph = computationResult.graph();
        LabelPropagation result = computationResult.result();

        WriteResultBuilder builder = new WriteResultBuilder(
            graph.nodeCount(),
            callContext,
            computationResult.tracker()
        );
        builder.withCreateMillis(computationResult.createMillis());
        builder.withComputeMillis(computationResult.computeMillis());
        builder.withConfig(config);

        if (!computationResult.isGraphEmpty()) {
            builder
                .didConverge(result.didConverge())
                .ranIterations(result.ranIterations())
                .withCommunityFunction((nodeId) -> result.labels().get(nodeId));
            if (shouldWrite(config)) {
                writeNodeProperties(builder, computationResult);
                graph.releaseProperties();
            }
        }
        return Stream.of(builder.build());
    }

    public static class WriteResult {

        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long postProcessingMillis;
        public long communityCount;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        WriteResult(
            long nodePropertiesWritten,
            long relationshipPropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long communityCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.relationshipPropertiesWritten = relationshipPropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.communityCount = communityCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }
    }

    public static class StatsResult {

        public long createMillis;
        public long computeMillis;
        public long postProcessingMillis;
        public long communityCount;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long communityCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.communityCount = communityCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        public static StatsResult from(WriteResult writeResult) {
            return new StatsResult(
                writeResult.createMillis,
                writeResult.computeMillis,
                writeResult.postProcessingMillis,
                writeResult.communityCount,
                writeResult.ranIterations,
                writeResult.didConverge,
                writeResult.communityDistribution,
                writeResult.configuration
            );
        }
    }

    static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {

        private long ranIterations;
        private boolean didConverge;

        WriteResultBuilder(
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(
                nodeCount,
                context,
                tracker
            );
        }

        WriteResultBuilder ranIterations(long iterations) {
            this.ranIterations = iterations;
            return this;
        }

        WriteResultBuilder didConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                nodePropertiesWritten,
                0L,
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                maybeCommunityCount.orElse(-1L),
                ranIterations,
                didConverge,
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }
}
