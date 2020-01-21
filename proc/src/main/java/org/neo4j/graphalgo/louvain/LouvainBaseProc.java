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
package org.neo4j.graphalgo.louvain;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class LouvainBaseProc<CONFIG extends LouvainBaseConfig> extends AlgoBaseProc<Louvain, Louvain, CONFIG> {

    static final String LOUVAIN_DESCRIPTION =
        "The Louvain method for community detection is an algorithm for detecting communities in networks.";

    @Override
    protected final LouvainFactory<CONFIG> algorithmFactory(CONFIG config) {
        return new LouvainFactory<>();
    }

    protected Stream<WriteResult> write(
        ComputationResult<Louvain, Louvain, CONFIG> computeResult
    ) {
        CONFIG config = computeResult.config();
        boolean write = config instanceof LouvainWriteConfig;
        LouvainWriteConfig writeConfig = ImmutableLouvainWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();
        if (computeResult.isGraphEmpty()) {
            return Stream.of(
                new WriteResult(
                    writeConfig,
                    0, computeResult.createMillis(),
                    0, 0, 0, 0, 0, 0,
                    new double[0], Collections.emptyMap()
                )
            );
        }

        Graph graph = computeResult.graph();
        Louvain louvain = computeResult.algorithm();

        WriteResultBuilder builder = new WriteResultBuilder(
            writeConfig,
            graph.nodeCount(),
            callContext,
            computeResult.tracker()
        );

        builder.withCreateMillis(computeResult.createMillis());
        builder.withComputeMillis(computeResult.computeMillis());
        builder
            .withLevels(louvain.levels())
            .withModularity(louvain.modularities()[louvain.levels() - 1])
            .withModularities(louvain.modularities())
            .withCommunityFunction(louvain::getCommunity);

        if (write && !writeConfig.writeProperty().isEmpty()) {
            writeNodeProperties(builder, computeResult);
            graph.releaseProperties();
        }

        return Stream.of(builder.build());
    }

    public static final class WriteResult {

        public String writeProperty;
        public String seedProperty;
        public String relationshipWeightProperty;
        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long postProcessingMillis;
        public long maxIterations;
        public long maxLevels;
        public double tolerance;
        public long ranLevels;
        public long communityCount;
        public boolean includeIntermediateCommunities;
        public double modularity;
        public List<Double> modularities;
        public Map<String, Object> communityDistribution;

        WriteResult(
            LouvainWriteConfig config,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long ranLevels,
            long communityCount,
            double modularity,
            double[] modularities,
            Map<String, Object> communityDistribution
        ) {
            this.relationshipPropertiesWritten = 0;

            this.writeProperty = config.writeProperty();
            this.seedProperty = config.seedProperty();
            this.relationshipWeightProperty = config.relationshipWeightProperty();
            this.maxIterations = config.maxIterations();
            this.maxLevels = config.maxLevels();
            this.tolerance = config.tolerance();
            this.includeIntermediateCommunities = config.includeIntermediateCommunities();

            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.ranLevels = ranLevels;
            this.communityCount = communityCount;
            this.modularity = modularity;
            this.modularities = Arrays.stream(modularities).boxed().collect(Collectors.toList());
            this.communityDistribution = communityDistribution;
        }
    }

    static class WriteResultBuilder extends AbstractCommunityResultBuilder<LouvainWriteConfig, WriteResult> {

        private long levels = -1;
        private double[] modularities = new double[]{};
        private double modularity = -1;

        WriteResultBuilder(
            LouvainWriteConfig config,
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(
                config,
                nodeCount,
                context,
                tracker
            );
        }

        WriteResultBuilder withLevels(long levels) {
            this.levels = levels;
            return this;
        }

        WriteResultBuilder withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        WriteResultBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                config,
                nodePropertiesWritten,  // should be nodePropertiesWritten
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                levels,
                maybeCommunityCount.orElse(-1L),
                modularity,
                modularities,
                communityHistogramOrNull()
            );
        }
    }
}
