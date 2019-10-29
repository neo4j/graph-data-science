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

import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.NullPropertyMap;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.wcc.WCC;
import org.neo4j.graphalgo.impl.wcc.WCCFactory;
import org.neo4j.graphalgo.impl.wcc.WCCType;
import org.neo4j.graphdb.Direction;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.wcc.WCCFactory.CONFIG_ALGO_TYPE;
import static org.neo4j.graphalgo.impl.wcc.WCCFactory.CONFIG_SEED_PROPERTY;

public abstract class WccBaseProc<T extends WCC<T>> extends BaseAlgoProc<T> {

    private static final String CONFIG_WRITE_PROPERTY = "writeProperty";
    private static final String CONFIG_OLD_WRITE_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";
    private static final String CONFIG_CONSECUTIVE_IDS_PROPERTY = "consecutiveIds";

    @Override
    protected GraphLoader configureAlgoLoader(final GraphLoader loader, final ProcedureConfiguration config) {
        final String seedProperty = config.getString(CONFIG_SEED_PROPERTY, null);
        if (seedProperty != null) {
            loader.withOptionalNodeProperties(createPropertyMappings(seedProperty));
        }

        WCCType algoType = config.getChecked(CONFIG_ALGO_TYPE, WCCType.PARALLEL, WCCType.class);

        if (algoType == WCCType.PREGEL) {
            loader.withDirection(Direction.BOTH);
        } else {
            loader.withDirection(Direction.OUTGOING);
        }

        return loader;
    }

    @Override
    protected WCCFactory<T> algorithmFactory(final ProcedureConfiguration config) {
        boolean incremental = config.getString(CONFIG_SEED_PROPERTY).isPresent();
        WCCType defaultAlgoType = WCCType.PARALLEL;
        WCCType algoType = config.getChecked(CONFIG_ALGO_TYPE, defaultAlgoType, WCCType.class);
        return new WCCFactory<>(algoType, incremental);
    }

    @Override
    protected double getDefaultWeightProperty(ProcedureConfiguration config) {
        return WCC.defaultWeight(config.get(WCCFactory.CONFIG_THRESHOLD, 0D));
    }

    protected Stream<StreamResult> stream(
        String label,
        String relationship,
        Map<String, Object> config,
        WCCType algoType
    ) {

        ProcedureSetup setup = setup(label, relationship, config, algoType);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.empty();
        }

        DisjointSetStruct dss = compute(setup);

        WccResultProducer producer = getResultProducer(
            dss,
            getSeedProperties(setup.graph, setup.procedureConfig),
            setup.procedureConfig,
            setup.tracker
        );

        return producer.resultStream(setup.graph);
    }

    protected Stream<WriteResult> run(
        String label,
        String relationship,
        Map<String, Object> config,
        WCCType algoType
    ) {

        ProcedureSetup setup = setup(label, relationship, config, algoType);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.of(WriteResult.EMPTY);
        }

        DisjointSetStruct communities = compute(setup);
        setup.builder.withCommunityFunction(communities::setIdOf);

        if (setup.procedureConfig.isWriteFlag()) {
            String writeProperty = setup.procedureConfig.get(
                CONFIG_WRITE_PROPERTY,
                CONFIG_OLD_WRITE_PROPERTY,
                DEFAULT_CLUSTER_PROPERTY
            );
            setup.builder.withWrite(true);
            setup.builder.withPartitionProperty(writeProperty).withWriteProperty(writeProperty);

            write(
                setup.builder::timeWrite,
                setup.graph,
                communities,
                setup.procedureConfig,
                writeProperty,
                setup.tracker
            );

            setup.graph.releaseProperties();
        }

        return Stream.of(setup.builder.build());
    }

    private ProcedureSetup setup(
        String label,
        String relationship,
        Map<String, Object> config,
        WCCType algoType
    ) {
        final AllocationTracker tracker = AllocationTracker.create();
        final WriteResultBuilder builder = new WriteResultBuilder(callContext.outputFields(), tracker);

        config.put(CONFIG_ALGO_TYPE, algoType);
        ProcedureConfiguration configuration = newConfig(label, relationship, config);

        Graph graph = loadGraph(configuration, tracker, builder);

        return new ProcedureSetup(builder, graph, tracker, configuration);
    }

    private PropertyMapping[] createPropertyMappings(String seedProperty) {
        return new PropertyMapping[]{
            PropertyMapping.of(seedProperty, -1),
        };
    }

    private NodeProperties getSeedProperties(Graph graph, ProcedureConfiguration config) {
        return graph.nodeProperties(config.getString(CONFIG_SEED_PROPERTY).orElse(null));
    }

    private void write(
        Supplier<ProgressTimer> timer,
        Graph graph,
        DisjointSetStruct struct,
        ProcedureConfiguration configuration,
        String writeProperty,
        AllocationTracker tracker
    ) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration, writeProperty, tracker);
        }
    }

    private void write(
        Graph graph,
        DisjointSetStruct dss,
        ProcedureConfiguration procedureConfiguration,
        String writeProperty,
        AllocationTracker tracker
    ) {
        log.debug("Writing results");

        WccResultProducer producer = getResultProducer(
            dss,
            getSeedProperties(graph, procedureConfiguration),
            procedureConfiguration,
            tracker
        );

        Exporter exporter = Exporter.of(api, graph)
            .withLog(log)
            .parallel(
                Pools.DEFAULT,
                procedureConfiguration.getWriteConcurrency(),
                TerminationFlag.wrap(transaction)
            )
            .build();
        exporter.write(
            writeProperty,
            producer,
            producer.getPropertyTranslator()
        );
    }

    protected abstract String name();

    private DisjointSetStruct compute(final ProcedureSetup setup) {

        T algo = newAlgorithm(setup.graph, setup.procedureConfig, setup.tracker);
        DisjointSetStruct algoResult = runWithExceptionLogging(
            name() + " failed",
            () -> setup.builder.timeEval((Supplier<DisjointSetStruct>) algo::compute)
        );

        log.info(name() + ": overall memory usage: %s", setup.tracker.getUsageString());

        algo.release();
        setup.graph.releaseTopology();

        return algoResult;
    }

    private WccResultProducer getResultProducer(
        final DisjointSetStruct dss,
        final NodeProperties seedProperties,
        final ProcedureConfiguration procedureConfiguration,
        final AllocationTracker tracker
    ) {
        String writeProperty = procedureConfiguration.get(
            CONFIG_WRITE_PROPERTY,
            CONFIG_OLD_WRITE_PROPERTY,
            DEFAULT_CLUSTER_PROPERTY
        );

        boolean withConsecutiveIds = procedureConfiguration.get(CONFIG_CONSECUTIVE_IDS_PROPERTY, false);
        boolean writePropertyEqualsSeedProperty = Objects.equals(seedProperties, writeProperty);
        boolean hasSeedProperties = seedProperties != null && !(seedProperties instanceof NullPropertyMap);

        WccResultProducer resultProducer = new WccResultProducer.NonConsecutive(
            WccResultProducer.NonSeedingTranslator.INSTANCE,
            dss
        );

        if (withConsecutiveIds && !hasSeedProperties) {
            resultProducer = new WccResultProducer.Consecutive(
                WccResultProducer.NonSeedingTranslator.INSTANCE,
                dss,
                tracker
            );
        } else if (writePropertyEqualsSeedProperty && hasSeedProperties) {
            resultProducer = new WccResultProducer.NonConsecutive(
                new PropertyTranslator.OfLongIfChanged<>(seedProperties, WccResultProducer::setIdOf),
                dss
            );
        }

        return resultProducer;
    }

    public static class ProcedureSetup {
        final WriteResultBuilder builder;
        final Graph graph;
        final AllocationTracker tracker;
        final ProcedureConfiguration procedureConfig;

        ProcedureSetup(
            final WriteResultBuilder builder,
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

    public static class StreamResult {

        public final long nodeId;

        public final long setId;

        public StreamResult(long nodeId, long setId) {
            this.nodeId = nodeId;
            this.setId = setId;
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
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            false, null, null
        );

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final long communityCount;
        public final long setCount;
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
        public final String partitionProperty;
        public final String writeProperty;

        WriteResult(
            long loadMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodes,
            long communityCount,
            long p100,
            long p99,
            long p95,
            long p90,
            long p75,
            long p50,
            long p25,
            long p10,
            long p5,
            long p1,
            boolean write,
            String partitionProperty,
            String writeProperty
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodes = nodes;
            this.communityCount = this.setCount = communityCount;
            this.p100 = p100;
            this.p99 = p99;
            this.p95 = p95;
            this.p90 = p90;
            this.p75 = p75;
            this.p50 = p50;
            this.p25 = p25;
            this.p10 = p10;
            this.p5 = p5;
            this.p1 = p1;
            this.write = write;
            this.partitionProperty = partitionProperty;
            this.writeProperty = writeProperty;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {
        private String partitionProperty;

        WriteResultBuilder(Stream<String> returnFields, AllocationTracker tracker) {
            super(returnFields, tracker);
        }

        WriteResultBuilder withPartitionProperty(String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                loadMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
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
                write,
                partitionProperty,
                writeProperty
            );
        }
    }
}
