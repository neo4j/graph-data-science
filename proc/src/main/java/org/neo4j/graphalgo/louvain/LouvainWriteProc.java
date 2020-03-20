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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.louvain.LouvainProc.LOUVAIN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LouvainWriteProc extends WriteProc<Louvain, Louvain, LouvainWriteProc.WriteResult, LouvainWriteConfig> {

    @Procedure(value = "gds.louvain.write", mode = WRITE)
    @Description(LOUVAIN_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.louvain.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PropertyTranslator<Louvain> nodePropertyTranslator(ComputationResult<Louvain, Louvain, LouvainWriteConfig> computationResult) {
        return LouvainProc.nodePropertyTranslator(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<Louvain, Louvain, LouvainWriteConfig> computeResult) {
        return LouvainProc.resultBuilder(new WriteResult.Builder(
            callContext, computeResult.tracker()),
            computeResult
        );
    }

    @Override
    protected LouvainWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LouvainWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Louvain, LouvainWriteConfig> algorithmFactory(LouvainWriteConfig config) {
        return new LouvainFactory<>();
    }

    static final class CommunityTranslator implements PropertyTranslator.OfLong<Louvain> {
        public static final CommunityTranslator INSTANCE = new CommunityTranslator();

        @Override
        public long toLong(Louvain louvain, long nodeId) {
            return louvain.getCommunity(nodeId);
        }
    }

    static final class CommunitiesTranslator implements PropertyTranslator.OfLongArray<Louvain> {
        public static final CommunitiesTranslator INSTANCE = new CommunitiesTranslator();

        @Override
        public long[] toLongArray(Louvain louvain, long nodeId) {
            return louvain.getCommunities(nodeId);
        }
    }

    public static final class WriteResult {

        public long nodePropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long postProcessingMillis;
        public long ranLevels;
        public long communityCount;
        public double modularity;
        public List<Double> modularities;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        WriteResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long ranLevels,
            long communityCount,
            double modularity,
            double[] modularities,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration

        ) {
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
            this.configuration = configuration;
        }

        static class Builder extends LouvainProc.LouvainResultBuilder<WriteResult> {

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
            protected WriteResult buildResult() {
                return new WriteResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingDuration,
                    levels,
                    maybeCommunityCount.orElse(-1L),
                    modularity,
                    modularities,
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
