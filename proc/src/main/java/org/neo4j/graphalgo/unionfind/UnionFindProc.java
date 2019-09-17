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
package org.neo4j.graphalgo.unionfind;

import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.huge.loader.NullWeightMap;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphalgo.impl.unionfind.UnionFind;
import org.neo4j.graphalgo.impl.unionfind.UnionFindFactory;
import org.neo4j.graphalgo.impl.unionfind.UnionFindType;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_ALGO_TYPE;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_SEED_PROPERTY;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.SEED_TYPE;

public class UnionFindProc<T extends UnionFind<T>> extends BaseAlgoProc<T> {

    private static final String CONFIG_WRITE_PROPERTY = "writeProperty";
    private static final String CONFIG_OLD_WRITE_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";
    private static final String CONFIG_CONSECUTIVE_IDS_PROPERTY = "consecutiveIds";

    @Procedure(value = "algo.unionFind", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindType.PARALLEL);
    }

    @Procedure(value = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{weightProperty: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', consecutiveId: false}}} " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.PARALLEL);
    }

    @Procedure(value = "algo.unionFind.memrec", mode = Mode.READ)
    @Description("CALL algo.unionFind.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for UnionFind")
    public Stream<MemRecResult> unionFindMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.queue", mode = Mode.WRITE, deprecatedBy = "algo.unionFind")
    @Description("CALL algo.unionFind.queue(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindQueue(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.queue.stream", deprecatedBy = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.queue.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindQueueStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoinMerge", mode = Mode.WRITE, deprecatedBy = "algo.unionFind")
    @Description("CALL algo.unionFind.forkJoinMerge(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindForkJoinMerge(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindType.FJ_MERGE);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoinMerge.stream", deprecatedBy = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.forkJoinMerge.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindForkJoinMergeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.FJ_MERGE);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoin", mode = Mode.WRITE, deprecatedBy = "algo.unionFind")
    @Description("CALL algo.unionFind.forkJoin(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindForkJoin(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindType.FORK_JOIN);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoin.stream", deprecatedBy = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.forkJoin.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindForJoinStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.FORK_JOIN);
    }

    @Override
    protected GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {

        final String seedProperty = config.getString(CONFIG_SEED_PROPERTY, null);

        if (seedProperty != null) {
            loader.withOptionalNodeProperties(createPropertyMappings(seedProperty));
        }

        return loader
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING);
    }

    @Override
    protected UnionFindFactory<T> algorithmFactory(final ProcedureConfiguration config) {
        boolean incremental = config.getString(CONFIG_SEED_PROPERTY).isPresent();
        UnionFindType defaultAlgoType = UnionFindType.PARALLEL;
        UnionFindType algoType = config.getChecked(CONFIG_ALGO_TYPE, defaultAlgoType, UnionFindType.class);
        return new UnionFindFactory<>(algoType, incremental);
    }

    private Stream<StreamResult> stream(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindType algoType) {

        ProcedureSetup setup = setup(label, relationship, config, algoType);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.empty();
        }

        DisjointSetStruct dss = compute(setup);

        UnionFindResultProducer producer = getResultProducer(
                dss,
                setup.graph.nodeProperties(SEED_TYPE),
                setup.procedureConfig, setup.tracker);

        return producer.resultStream(setup.graph);
    }

    private Stream<WriteResult> run(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindType algoType) {

        ProcedureSetup setup = setup(label, relationship, config, algoType);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.of(WriteResult.EMPTY);
        }

        DisjointSetStruct communities = compute(setup);

        if (setup.procedureConfig.isWriteFlag()) {
            String writeProperty = setup.procedureConfig.get(
                    CONFIG_WRITE_PROPERTY,
                    CONFIG_OLD_WRITE_PROPERTY,
                    DEFAULT_CLUSTER_PROPERTY);
            setup.builder.withWrite(true);
            setup.builder.withPartitionProperty(writeProperty).withWriteProperty(writeProperty);

            write(
                    setup.builder::timeWrite,
                    setup.graph,
                    communities,
                    setup.procedureConfig,
                    writeProperty,
                    setup.tracker);

            setup.graph.releaseProperties();
        }

        return Stream.of(setup.builder.build(setup.tracker, setup.graph.nodeCount(), communities::setIdOf));
    }

    private ProcedureSetup setup(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindType algoType) {
        final WriteResultBuilder builder = new WriteResultBuilder(callContext.outputFields());

        config.put(CONFIG_ALGO_TYPE, algoType);
        ProcedureConfiguration configuration = newConfig(label, relationship, config);

        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = loadGraph(configuration, tracker, builder);
        return new ProcedureSetup(builder, graph, tracker, configuration);
    }

    private PropertyMapping[] createPropertyMappings(String seedProperty) {
        return new PropertyMapping[]{
                PropertyMapping.of(SEED_TYPE, seedProperty, -1),
        };
    }

    private void write(
            Supplier<ProgressTimer> timer,
            Graph graph,
            DisjointSetStruct struct,
            ProcedureConfiguration configuration,
            String writeProperty,
            AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration, writeProperty, tracker);
        }
    }

    private void write(
            Graph graph,
            DisjointSetStruct dss,
            ProcedureConfiguration procedureConfiguration,
            String writeProperty,
            AllocationTracker tracker) {
        log.debug("Writing results");

        UnionFindResultProducer producer = getResultProducer(
                dss,
                graph.nodeProperties(SEED_TYPE),
                procedureConfiguration, tracker);

        Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(
                        Pools.DEFAULT,
                        procedureConfiguration.getWriteConcurrency(),
                        TerminationFlag.wrap(transaction))
                .build();
        exporter.write(
                writeProperty,
                producer,
                producer.getPropertyTranslator());
    }

    private DisjointSetStruct compute(final ProcedureSetup setup) {

        T algo = newAlgorithm(setup.graph, setup.procedureConfig, setup.tracker);
        DisjointSetStruct algoResult = runWithExceptionLogging(
                "UnionFind failed",
                () -> setup.builder.timeEval((Supplier<DisjointSetStruct>) algo::compute));

        log.info("UnionFind: overall memory usage: %s", setup.tracker.getUsageString());

        algo.release();
        setup.graph.releaseTopology();

        return algoResult;
    }

    private UnionFindResultProducer getResultProducer(
            final DisjointSetStruct dss,
            final WeightMapping nodeProperties,
            final ProcedureConfiguration procedureConfiguration,
            final AllocationTracker tracker) {
        String writeProperty = procedureConfiguration.get(
                CONFIG_WRITE_PROPERTY,
                CONFIG_OLD_WRITE_PROPERTY,
                DEFAULT_CLUSTER_PROPERTY);
        String seedProperty = procedureConfiguration.getString(CONFIG_SEED_PROPERTY, null);

        boolean withConsecutiveIds = procedureConfiguration.get(CONFIG_CONSECUTIVE_IDS_PROPERTY, false);
        boolean withSeeding = seedProperty != null;
        boolean writePropertyEqualsSeedProperty = Objects.equals(seedProperty, writeProperty);
        boolean hasNodeProperties = nodeProperties != null && !(nodeProperties instanceof NullWeightMap);

        UnionFindResultProducer resultProducer = new UnionFindResultProducer.NonConsecutive(
                UnionFindResultProducer.NonSeedingTranslator.INSTANCE,
                dss);

        if (withConsecutiveIds && !withSeeding) {
            resultProducer = new UnionFindResultProducer.Consecutive(
                    UnionFindResultProducer.NonSeedingTranslator.INSTANCE,
                    dss,
                    tracker);
        } else if (writePropertyEqualsSeedProperty && hasNodeProperties) {
            resultProducer = new UnionFindResultProducer.NonConsecutive(
                    new PropertyTranslator.OfLongIfChanged<>(nodeProperties, UnionFindResultProducer::setIdOf),
                    dss);
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
                final ProcedureConfiguration procedureConfig) {
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
                false, null, null);

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
                String writeProperty) {
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

    static class WriteResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {
        private String partitionProperty;
        private String writeProperty;

        WriteResultBuilder(Set<String> returnFields) {
            super(returnFields);
        }

        WriteResultBuilder(Stream<String> returnFields) {
            super(returnFields);
        }

        @Override
        protected WriteResult build(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount,
                OptionalLong maybeCommunityCount,
                Optional<Histogram> maybeCommunityHistogram,
                boolean write) {


            return new WriteResult(
                    loadMillis,
                    computeMillis,
                    postProcessingMillis,
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

        WriteResultBuilder withPartitionProperty(String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        WriteResultBuilder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }

}
