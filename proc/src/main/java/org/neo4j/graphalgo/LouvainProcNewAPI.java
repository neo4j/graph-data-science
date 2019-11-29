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
package org.neo4j.graphalgo;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainFactoryNew;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.LouvainConfig;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LouvainProcNewAPI extends BaseAlgoProc<Louvain, LouvainConfig> {

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
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        Pair<LouvainConfig, Optional<String>> input = processInput(
            graphNameOrConfig,
            configuration
        );
        Graph graph = loadGraph(input);

        return run(graph, input.first(), true);
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
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        Pair<LouvainConfig, Optional<String>> input = processInput(graphNameOrConfig, configuration);
        Graph graph = loadGraph(input);

        return run(graph, input.first(), false);
    }

    @Override
    protected LouvainConfig newConfig(Optional<String> graphName, CypherMapWrapper config) {
        Optional<GraphCreateConfig> maybeImplicitCreate = Optional.empty();
        if (!graphName.isPresent()) {
            // we should do implicit loading
            maybeImplicitCreate = Optional.of(GraphCreateConfig.implicitCreate(getUsername(), config));
        }
        return LouvainConfig.of(getUsername(), graphName, maybeImplicitCreate, config);
    }


    public static final class WriteResult {

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
            LouvainConfig config,
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

    public Stream<WriteResult> run(Graph graph, LouvainConfig config, boolean write) {
        AllocationTracker tracker = AllocationTracker.create();
        WriteResultBuilder builder = new WriteResultBuilder(config, callContext, tracker);
        builder.withNodeCount(graph.nodeCount());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }
        MutableLong computeMillis = new MutableLong(0);
        Louvain louvain = compute(graph, config, computeMillis::setValue, tracker);

        builder
            .withLevels(louvain.levels())
            .withModularity(louvain.modularities()[louvain.levels() -1])
            .withModularities(louvain.modularities())
            .withCommunityFunction(louvain::getCommunity);

        if (write && !config.writeProperty().isEmpty()) {
            write(
                builder::timeWrite,
                graph,
                config,
                louvain,
                louvain.terminationFlag
            );

            graph.releaseProperties();
        }

        return Stream.of(builder.build());
    }

    @Override
    protected LouvainFactoryNew algorithmFactory(LouvainConfig config) {
        Louvain.Config louvainConfig = new Louvain.Config(
            config.maxLevels(),
            config.maxIterations(),
            config.tolerance(),
            config.includeIntermediateCommunities(),
            Optional.ofNullable(config.seedProperty())
        );

        return new LouvainFactoryNew(louvainConfig);
    }

    private Louvain compute(
        Graph graph,
        LouvainConfig config,
        LongConsumer timer,
        AllocationTracker tracker
    ) {
        final Louvain louvain = newAlgorithm(graph, config, tracker);
        runWithExceptionLogging(
            Louvain.class.getSimpleName() + " failed",
            () -> {
                try (ProgressTimer ignored = ProgressTimer.start(timer)) {
                    louvain.compute();
                }
            }
        );

        log.info(Louvain.class.getSimpleName() + ": overall memory usage %s", tracker.getUsageString());

        louvain.release();
        graph.releaseTopology();
        return louvain;
    }

    private void write(
        Supplier<ProgressTimer> timer,
        Graph graph,
        LouvainConfig config,
        Louvain louvain,
        TerminationFlag terminationFlag
    ) {
        try (ProgressTimer ignored = timer.get()) {
            log.debug("Writing results");

            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();

            Optional<NodeProperties> seed = louvain.config().maybeSeedPropertyKey.map(graph::nodeProperties);
            PropertyTranslator<Louvain> translator;
            if (!louvain.config().includeIntermediateCommunities) {
                if (seed.isPresent() && config.seedProperty().equals(config.writeProperty())) {
                    translator = new PropertyTranslator.OfLongIfChanged<>(seed.get(), Louvain::getCommunity);
                } else {
                    translator = CommunityTranslator.INSTANCE;
                }
            } else {
                translator = CommunitiesTranslator.INSTANCE;
            }

            exporter.write(
                config.writeProperty(),
                louvain,
                translator
            );
        }
    }

    public static final class StreamResult {
        public final long nodeId;
        public final List<Long> communities;
        public final long community;

        StreamResult(final long nodeId, final long[] communities, final long community) {
            this.nodeId = nodeId;
            this.communities = communities == null ? null : Arrays.stream(communities).boxed().collect(Collectors.toList());
            this.community = community;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {

        private final LouvainConfig config;

        private long levels = -1;
        private long ranIterations;
        private double[] modularities = new double[]{};
        private double modularity = -1;

        WriteResultBuilder(LouvainConfig config, ProcedureCallContext context, AllocationTracker tracker) {
            super(
                // TODO: factor these out to OutputFieldParser
                context.outputFields().anyMatch(s -> s.equalsIgnoreCase("communityDistribution")),
                context.outputFields().anyMatch(s -> s.equalsIgnoreCase("communityCount")),
                tracker
            );
            this.config = config;
        }

        LouvainProcNewAPI.WriteResultBuilder ranIterations(long iterations) {
            this.ranIterations = iterations;
            return this;
        }

        LouvainProcNewAPI.WriteResultBuilder withLevels(long levels) {
            this.levels = levels;
            return this;
        }

        LouvainProcNewAPI.WriteResultBuilder withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        LouvainProcNewAPI.WriteResultBuilder withModularity(double modularity) {
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

    static final class CommunityTranslator implements PropertyTranslator.OfLong<Louvain>  {
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
