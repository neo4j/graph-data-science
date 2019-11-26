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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainFactory;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.SEED_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.WRITE_PROPERTY_KEY;
import static org.neo4j.graphalgo.impl.louvain.LouvainFactory.DEFAULT_LOUVAIN_DIRECTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LouvainProc extends BaseAlgoProc<Louvain> {

    public static final String LEVELS_KEY = "levels";
    public static final int LEVELS_DEFAULT = 10;
    public static final String INNER_ITERATIONS_KEY = "innerIterations";
    public static final int INNER_ITERATIONS_DEFAULT = 10;
    public static final String INCLUDE_INTERMEDIATE_COMMUNITIES_KEY = "includeIntermediateCommunities";
    public static final boolean INCLUDE_INTERMEDIATE_COMMUNITIES_DEFAULT = false;
    public static final String LEGACY_COMMUNITY_PROPERTY_KEY = "communityProperty";

    @Procedure(value = "algo.beta.louvain", mode = WRITE)
    @Description("CALL algo.beta.louvain(label:String, relationship:String, " +
                 "{levels: 10, innerIterations: 10, tolerance: 0.00001, weightProperty: 'weight', seedProperty: 'seed', write: true, writeProperty: 'community', includeIntermediateCommunities: false, concurrency: 4 }) " +
                 "YIELD nodes, communityCount, levels, modularity, modularities, write, writeProperty, includeIntermediateCommunities, loadMillis, computeMillis, writeMillis, postProcessingMillis")
    public Stream<WriteResult> louvainWrite(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipTypes,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return run(label, relationshipTypes, config);
    }

    @Procedure(value = "algo.beta.louvain.stream", mode = READ)
    @Description("CALL algo.beta.louvain.stream(label:String, relationship:String, " +
                 "{levels: 10, innerIterations: 10, tolerance: 0.00001, weightProperty: 'weight', seedProperty: 'seed', includeIntermediateCommunities: false, concurrency: 4 }) " +
                 "YIELD nodeId, community, communities - yields a community id for each node id")
    public Stream<StreamResult> louvainStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipTypes,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationshipTypes, config);
    }

    @Procedure(value = "algo.louvain", mode = WRITE)
    @Description("CALL algo.louvain(label:String, relationship:String, " +
                 "{weightProperty: 'weight', defaultValue: 1.0, write: true, writeProperty: 'community', concurrency: 4, communityProperty: 'propertyOfPredefinedCommunity', innerIterations: 10, communitySelection: 'classic'}) " +
                 "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis")
    public Stream<LegacyWriteResult> writeLegacy(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipTypes,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        Object communityProperty = config.get(LEGACY_COMMUNITY_PROPERTY_KEY);
        config.put(SEED_PROPERTY_KEY, communityProperty);

        Stream<WriteResult> resultStream = run(label, relationshipTypes, config);
        return resultStream.map(LegacyWriteResult::fromWriteResult);
    }

    @Procedure(value = "algo.louvain.stream", mode = READ)
    @Description("CALL algo.louvain.stream(label:String, relationship:String, " +
                 "{levels: 10, innerIterations: 10, weightProperty: 'weight', seedProperty: 'seed', includeIntermediateCommunities: false, concurrency: 4}) " +
                 "YIELD nodeId, community, communities - yields a community id for each node id")
    public Stream<StreamResult> louvainStreamLegacy(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipTypes,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return louvainStream(label, relationshipTypes, config);
    }

    @Procedure(value = "algo.louvain.memrec", mode = READ)
    @Description("CALL algo.louvain.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for Louvain")
    public Stream<MemRecResult> louvainMemrec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    public Stream<WriteResult> run(String label, String relationshipType, Map<String, Object> config) {
        LouvainProc.ProcedureSetup setup = setup(label, relationshipType, config);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.of(WriteResult.EMPTY);
        }

        Louvain louvain = compute(setup);

        setup.builder.withCommunityFunction(louvain::getCommunity);

        Optional<String> writeProperty = setup.procedureConfig.getString(WRITE_PROPERTY_KEY);

        setup.builder
            .withLevels(louvain.levels())
            .withModularity(louvain.modularities()[louvain.levels() -1])
            .withModularities(louvain.modularities())
            .withIncludeIntermediateCommunities(louvain.config().includeIntermediateCommunities);

        if (setup.procedureConfig.isWriteFlag() && writeProperty.isPresent() && !writeProperty.get().equals("")) {
            setup.builder.withWrite(true);
            setup.builder.withWriteProperty(writeProperty.get());

            write(
                setup.builder::timeWrite,
                setup.graph,
                louvain,
                setup.procedureConfig,
                writeProperty.get(),
                louvain.terminationFlag,
                setup.tracker
            );

            setup.graph.releaseProperties();
        }

        return Stream.of(setup.builder.build());
    }

    public Stream<StreamResult> stream(
        String label,
        String relationship,
        Map<String, Object> config
    ) {

        LouvainProc.ProcedureSetup setup = setup(label, relationship, config);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.empty();
        }

        Louvain louvain = compute(setup);

        return LongStream.range(0, setup.graph.nodeCount())
            .mapToObj(nodeId -> {
                long neoNodeId = setup.graph.toOriginalNodeId(nodeId);
                long[] communities = louvain.config().includeIntermediateCommunities ? louvain.getCommunities(nodeId) : null;
                return new LouvainProc.StreamResult(neoNodeId, communities, louvain.getCommunity(nodeId));
            });
    }

    @Override
    protected GraphLoader configureGraphLoader(GraphLoader loader, ProcedureConfiguration config) {
        final String seedProperty = config.getString(SEED_PROPERTY_KEY, null);
        if (seedProperty != null) {
            loader.withOptionalNodeProperties(PropertyMapping.of(seedProperty, -1));
        }

        return loader.withDirection(config.getDirection(DEFAULT_LOUVAIN_DIRECTION));
    }

    @Override
    protected AlgorithmFactory<Louvain> algorithmFactory(ProcedureConfiguration config) {
        Louvain.Config louvainConfig = new Louvain.Config(
            config.getInt(LEVELS_KEY, LEVELS_DEFAULT),
            config.getInt(INNER_ITERATIONS_KEY, INNER_ITERATIONS_DEFAULT),
            config.get(TOLERANCE_KEY, TOLERANCE_DEFAULT),
            config.getBool(INCLUDE_INTERMEDIATE_COMMUNITIES_KEY, INCLUDE_INTERMEDIATE_COMMUNITIES_DEFAULT),
            config.getString(SEED_PROPERTY_KEY)
        );

        return new LouvainFactory(louvainConfig);
    }

    private Louvain compute(ProcedureSetup setup) {
        final Louvain louvain = newAlgorithm(setup.graph, setup.procedureConfig, setup.tracker);
        runWithExceptionLogging(
            Louvain.class.getSimpleName() + " failed",
            () -> setup.builder.timeEval(louvain::compute)
        );

        log.info(Louvain.class.getSimpleName() + ": overall memory usage %s", setup.tracker.getUsageString());

        louvain.release();
        setup.graph.releaseTopology();

        return louvain;
    }

    private LouvainProc.ProcedureSetup setup(
        String label,
        String relationship,
        Map<String, Object> config
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        LouvainProc.WriteResultBuilder builder = new LouvainProc.WriteResultBuilder(configuration, tracker);

        Graph graph = loadGraph(configuration, tracker, builder);

        return new LouvainProc.ProcedureSetup(builder, graph, tracker, configuration);
    }

    private void write(
        Supplier<ProgressTimer> timer,
        Graph graph,
        Louvain louvain,
        ProcedureConfiguration configuration,
        String writeProperty,
        TerminationFlag terminationFlag,
        AllocationTracker tracker
    ) {
        try (ProgressTimer ignored = timer.get()) {
            log.debug("Writing results");

            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                .build();

            Optional<NodeProperties> seed = louvain.config().maybeSeedPropertyKey.map(graph::nodeProperties);
            PropertyTranslator<Louvain> translator;
            if (!louvain.config().includeIntermediateCommunities) {
                if (seed.isPresent() && configuration.getString(SEED_PROPERTY_KEY, "").equals(writeProperty)) {
                    translator = new PropertyTranslator.OfLongIfChanged<>(seed.get(), Louvain::getCommunity);
                } else {
                    translator = CommunityTranslator.INSTANCE;
                }
            } else {
               translator = CommunitiesTranslator.INSTANCE;
            }

            exporter.write(
                writeProperty,
                louvain,
                translator
            );
        }
    }

    public static class ProcedureSetup {
        final LouvainProc.WriteResultBuilder builder;
        final Graph graph;
        final AllocationTracker tracker;
        final ProcedureConfiguration procedureConfig;

        ProcedureSetup(
            final LouvainProc.WriteResultBuilder builder,
            final Graph graph,
            final AllocationTracker tracker,
            final ProcedureConfiguration procedureConfig
        ) {
            this.builder = builder;
            this.graph = graph;
            this.tracker = tracker;
            this.procedureConfig = procedureConfig;
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

    public static class WriteResult {

        public static final WriteResult EMPTY = new WriteResult(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            new double[0],
            false,
            null,
            false
        );

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final long communityCount;
        public final long levels;
        public final double modularity;
        public final List<Double> modularities;
        public final long p1;
        public final long p5;
        public final long p10;
        public final long p25;
        public final long p50;
        public final long p75;
        public final long p90;
        public final long p95;
        public final long p99;
        public final long p100;
        public final boolean write;
        public final String writeProperty;
        public final boolean includeIntermediateCommunities;

        public WriteResult(
            long loadMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodes,
            long communityCount,
            long p1,
            long p5,
            long p10,
            long p25,
            long p50,
            long p75,
            long p90,
            long p95,
            long p99,
            long p100,
            long levels,
            double modularity,
            double[] modularities,
            boolean write,
            String writeProperty,
            boolean includeIntermediateCommunities
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.communityCount = communityCount;
            this.levels = levels;
            this.modularity = modularity;
            this.modularities = Arrays.stream(modularities).boxed().collect(Collectors.toList());
            this.p1 = p1;
            this.p5 = p5;
            this.p10 = p10;
            this.p25 = p25;
            this.p50 = p50;
            this.p75 = p75;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.p100 = p100;
            this.write = write;
            this.writeProperty = writeProperty;
            this.includeIntermediateCommunities = includeIntermediateCommunities;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {

        private long levels = -1;
        private double[] modularities = new double[]{};
        private double modularity = -1;

        private boolean includeIntermediateCommunities;

        WriteResultBuilder(ProcedureConfiguration config, AllocationTracker tracker) {
            super(config.computeHistogram(), config.computeCommunityCount(), tracker);
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

        WriteResultBuilder withIncludeIntermediateCommunities(boolean includeIntermediateCommunities) {
            this.includeIntermediateCommunities = includeIntermediateCommunities;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                nodeCount,
                maybeCommunityCount.orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(100)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(99)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(95)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(90)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(75)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(50)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(25)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(10)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(5)).orElse(-1L),
                maybeCommunityHistogram.map(histogram -> histogram.getValueAtPercentile(1)).orElse(-1L),
                levels,
                modularity,
                modularities,
                write,
                writeProperty,
                includeIntermediateCommunities
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
