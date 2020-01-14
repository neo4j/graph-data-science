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

package org.neo4j.graphalgo.modularity;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.modularity.ModularityOptimization;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ModularityOptimizationWriteProc extends ModularityOptimizationBaseProc<ModularityOptimizationWriteConfig> {

    @Procedure(name = "gds.beta.modularityOptimization.write", mode = Mode.WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteConfig> computationResult =
            compute(graphNameOrConfig, configuration);

        // TODO product: check for an empty graph (not algorithm) and return a single "empty write result" value
        return computationResult.result() != null
            ? write(computationResult)
            : Stream.empty();
    }

    @Procedure(value = "gds.beta.modularityOptimization.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    private Stream<WriteResult> write(ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteConfig> computationResult) {
        ModularityOptimizationWriteConfig config = computationResult.config();
        ModularityOptimization result = computationResult.result();
        Graph graph = computationResult.graph();

        AbstractCommunityResultBuilder<ModularityOptimizationWriteConfig, WriteResult> builder = new WriteResultBuilder(config, graph.nodeCount(), callContext, computationResult.tracker())
            .withCommunityProperty(config.writeProperty())
            .withModularity(result.getModularity())
            .withRanIterations(result.getIterations())
            .withDidConverge(result.didConverge())
            .withCommunityFunction(result::getCommunityId);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        writeNodeProperties(builder, computationResult);

        graph.release();
        return Stream.of(builder.build());
    }

    @Override
    protected ModularityOptimizationWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ModularityOptimizationWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected PropertyTranslator<ModularityOptimization> nodePropertyTranslator(ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteConfig> computationResult) {
        return ModularityOptimizationTranslator.INSTANCE;
    }

    static final class ModularityOptimizationTranslator implements PropertyTranslator.OfLong<ModularityOptimization> {
        public static final ModularityOptimizationTranslator INSTANCE = new ModularityOptimizationTranslator();

        @Override
        public long toLong(ModularityOptimization data, long nodeId) {
            return data.getCommunityId(nodeId);
        }
    }

    public static class WriteResult {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final String communityProperty;
        public final String writeProperty;
        public boolean didConverge;
        public long ranIterations;
        public double modularity;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;

        WriteResult(
            long loadMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodes,
            String communityProperty,
            String writeProperty,
            boolean didConverge,
            long ranIterations,
            double modularity,
            long communityCount,
            Map<String, Object> communityDistribution
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodes = nodes;
            this.communityProperty = communityProperty;
            this.writeProperty = writeProperty;
            this.didConverge = didConverge;
            this.ranIterations = ranIterations;
            this.modularity = modularity;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<ModularityOptimizationWriteConfig, WriteResult> {
        private String communityProperty;
        private long ranIterations;
        private boolean didConverge;
        private double modularity;

        WriteResultBuilder(
            ModularityOptimizationWriteConfig config,
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(config, nodeCount, context, tracker);
        }

        WriteResultBuilder withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        WriteResultBuilder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        WriteResultBuilder withCommunityProperty(String communityProperty) {
            this.communityProperty = communityProperty;
            return this;
        }

        WriteResultBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                createMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodePropertiesWritten,
                communityProperty,
                writeProperty,
                didConverge,
                ranIterations,
                modularity,
                maybeCommunityCount.orElse(0),
                communityHistogramOrNull()
            );
        }
    }
}
