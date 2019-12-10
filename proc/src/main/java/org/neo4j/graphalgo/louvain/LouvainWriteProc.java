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
package org.neo4j.graphalgo.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LouvainWriteProc extends LouvainProcBase<LouvainWriteConfig> {

    @Procedure(value = "gds.algo.louvain.write", mode = WRITE)
    @Description("CALL gds.algo.louvain.write(graphName: STRING, configuration: MAP {" +
                 "    maxIteration: INTEGER" +
                 "    maxLevels: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    includeIntermediateCommunities: BOOLEAN" +
                 "    seedProperty: STRING" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER," +
                 "  maxIterations: INTEGER," +
                 "  maxLevels: INTEGER," +
                 "  includeIntermediateCommunities: BOOLEAN," +
                 "  seedProperty: STRING," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  communityCount: INTEGER," +
                 "  ranIterations: INTEGER," +
                 "  ranLevels: INTEGER," +
                 "  modularity: FLOAT," +
                 "  modularities: LIST OF FLOAT," +
                 "  didConverge: LIST OF BOOLEAN," +
                 "  communityDistribution: MAP")
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Louvain, Louvain, LouvainWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, true);
    }

    @Procedure(value = "gds.algo.louvain.stats", mode = READ)
    @Description("CALL gds.algo.louvain.stats(graphName: STRING, configuration: MAP {" +
                 "    maxIteration: INTEGER" +
                 "    maxLevels: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    includeIntermediateCommunities: BOOLEAN" +
                 "    seedProperty: STRING" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER," +
                 "  maxIterations: INTEGER," +
                 "  maxLevels: INTEGER," +
                 "  includeIntermediateCommunities: BOOLEAN," +
                 "  seedProperty: STRING," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  communityCount: INTEGER," +
                 "  ranIterations: INTEGER," +
                 "  ranLevels: INTEGER," +
                 "  modularity: FLOAT," +
                 "  modularities: LIST OF FLOAT," +
                 "  didConverge: LIST OF BOOLEAN," +
                 "  communityDistribution: MAP")
    public Stream<WriteResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Louvain, Louvain, LouvainWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, false);
    }

    @Procedure(value = "gds.algo.louvain.write.estimate", mode = READ)
    @Description("CALL gds.algo.louvain.write.estimate(graphName: STRING, configuration: MAP {" +
                 "    maxIteration: INTEGER" +
                 "    maxLevels: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    includeIntermediateCommunities: BOOLEAN" +
                 "    seedProperty: STRING" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.algo.louvain.stats.estimate", mode = READ)
    @Description("CALL gds.algo.louvain.stats.estimate(graphName: STRING, configuration: MAP {" +
                 "    maxIteration: INTEGER" +
                 "    maxLevels: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    includeIntermediateCommunities: BOOLEAN" +
                 "    seedProperty: STRING" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    private Stream<WriteResult> write(
        ComputationResult<Louvain, Louvain, LouvainWriteConfig> computeResult,
        boolean write
    ) {
        if (computeResult.isEmpty()) {
          return Stream.of(
              new WriteResult(
                  computeResult.config(),
                  0, computeResult.createMillis(),
                  0, 0, 0, 0, 0, 0, 0,
                  new double[0], Collections.emptyMap()
              )
          );
        } else {
            LouvainWriteConfig config = computeResult.config();
            Graph graph = computeResult.graph();
            Louvain louvain = computeResult.algorithm();

            WriteResultBuilder builder = new WriteResultBuilder(config, callContext, computeResult.tracker());

            builder.setLoadMillis(computeResult.createMillis());
            builder.setComputeMillis(computeResult.computeMillis());
            builder.withNodeCount(graph.nodeCount());
            builder
                .withLevels(louvain.levels())
                .withModularity(louvain.modularities()[louvain.levels() - 1])
                .withModularities(louvain.modularities())
                .withCommunityFunction(louvain::getCommunity);

            if (write && !config.writeProperty().isEmpty()) {
                writeNodeProperties(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    @Override
    protected Optional<PropertyTranslator<Louvain>> nodePropertyTranslator(ComputationResult<Louvain, Louvain, LouvainWriteConfig> computationResult) {
        Graph graph = computationResult.graph();
        Louvain louvain = computationResult.result();
        LouvainWriteConfig config = computationResult.config();
        Optional<NodeProperties> seed = Optional.ofNullable(louvain.config().seedProperty()).map(graph::nodeProperties);
        PropertyTranslator<Louvain> translator;
        if (!louvain.config().includeIntermediateCommunities()) {
            if (seed.isPresent() && Objects.equals(config.seedProperty(), config.writeProperty())) {
                translator = new PropertyTranslator.OfLongIfChanged<>(seed.get(), Louvain::getCommunity);
            } else {
                translator = CommunityTranslator.INSTANCE;
            }
        } else {
            translator = CommunitiesTranslator.INSTANCE;
        }
        return Optional.of(translator);
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

    public static final class WriteResult {

        // TODO: add tolerance
        public String writeProperty;
        public String seedProperty;
        public String weightProperty;
        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long postProcessingMillis;
        public long maxIterations;
        public long maxLevels;
        public long ranIterations;
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
            long ranIterations,
            long ranLevels,
            long communityCount,
            double modularity,
            double[] modularities,
            Map<String, Object> communityDistribution
        ) {
            this.relationshipPropertiesWritten = 0;

            this.writeProperty = config.writeProperty();
            this.seedProperty = config.seedProperty();
            this.weightProperty = config.weightProperty();
            this.maxIterations = config.maxIterations();
            this.maxLevels = config.maxLevels();
            this.includeIntermediateCommunities = config.includeIntermediateCommunities();

            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.ranIterations = ranIterations;
            this.ranLevels = ranLevels;
            this.communityCount = communityCount;
            this.modularity = modularity;
            this.modularities = Arrays.stream(modularities).boxed().collect(Collectors.toList());
            this.communityDistribution = communityDistribution;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {

        private final LouvainWriteConfig config;

        private long levels = -1;
        private long ranIterations;
        private double[] modularities = new double[]{};
        private double modularity = -1;

        WriteResultBuilder(LouvainWriteConfig config, ProcedureCallContext context, AllocationTracker tracker) {
            super(
                // TODO: factor these out to OutputFieldParser
                context.outputFields().anyMatch(s -> s.equalsIgnoreCase("communityDistribution")),
                context.outputFields().anyMatch(s -> s.equalsIgnoreCase("communityCount")),
                tracker
            );
            this.config = config;
        }

        LouvainWriteProc.WriteResultBuilder ranIterations(long iterations) {
            this.ranIterations = iterations;
            return this;
        }

        LouvainWriteProc.WriteResultBuilder withLevels(long levels) {
            this.levels = levels;
            return this;
        }

        LouvainWriteProc.WriteResultBuilder withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        LouvainWriteProc.WriteResultBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                config,
                nodeCount,  // should be nodePropertiesWritten
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                ranIterations,
                levels,
                maybeCommunityCount.orElse(-1L),
                modularity,
                modularities,
                communityHistogramOrNull()
            );
        }
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
}
