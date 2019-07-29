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

import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
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
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_ALGO_TYPE;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_SEED_PROPERTY;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.SEED_TYPE;

public class UnionFindProc<T extends UnionFind<T>> extends BaseAlgoProc<T> {

    private static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    private static final String CONFIG_OLD_CLUSTER_PROPERTY = "partitionProperty";
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
    public Stream<UnionFindProc.StreamResult> unionFindStream(
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
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
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
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<UnionFindProc.StreamResult> unionFindQueueStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoinMerge", mode = Mode.WRITE, deprecatedBy = "algo.unionFind")
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
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
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<UnionFindProc.StreamResult> unionFindForkJoinMergeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.FJ_MERGE);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoin", mode = Mode.WRITE, deprecatedBy = "algo.unionFind")
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
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
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<UnionFindProc.StreamResult> unionFindForJoinStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindType.FORK_JOIN);
    }

    @Override
    GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {

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
    UnionFindFactory<T> algorithmFactory(final ProcedureConfiguration config) {
        boolean incremental = config.getString(CONFIG_SEED_PROPERTY).isPresent();
        UnionFindType defaultAlgoType = UnionFindType.PARALLEL;
        UnionFindType algoType = config.getChecked(CONFIG_ALGO_TYPE, defaultAlgoType, UnionFindType.class);
        return new UnionFindFactory<>(algoType, incremental);
    }

    private Stream<UnionFindProc.StreamResult> stream(
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

        UnionFindResultProducer producer = getResultProducer(dss, setup.procedureConfig, setup.tracker);

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
                    CONFIG_CLUSTER_PROPERTY,
                    CONFIG_OLD_CLUSTER_PROPERTY,
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
        }

        return Stream.of(setup.builder.build(setup.tracker, setup.graph.nodeCount(), communities::setIdOf));
    }

    private ProcedureSetup setup(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindType algoType) {
        final Builder builder = new Builder();

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

        UnionFindResultProducer producer = getResultProducer(dss, procedureConfiguration, tracker);

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
                UnionFindResultProducer.Translator.INSTANCE);
    }

    private DisjointSetStruct compute(final ProcedureSetup procedureSetup) {

        T algo = newAlgorithm(procedureSetup.graph, procedureSetup.procedureConfig, procedureSetup.tracker);
        DisjointSetStruct algoResult = runWithExceptionLogging(
                "UnionFind failed",
                () -> procedureSetup.builder.timeEval((Supplier<DisjointSetStruct>) algo::compute));

        log.info("UnionFind: overall memory usage: %s", procedureSetup.tracker.getUsageString());

        algo.release();
        procedureSetup.graph.release();

        return algoResult;
    }

    private UnionFindResultProducer getResultProducer(
            final DisjointSetStruct dss,
            final ProcedureConfiguration procedureConfiguration,
            final AllocationTracker tracker) {
        boolean withConsecutiveIds = procedureConfiguration.get(CONFIG_CONSECUTIVE_IDS_PROPERTY, false);
        boolean withSeeding = procedureConfiguration.get(CONFIG_SEED_PROPERTY, null) != null;

        return withConsecutiveIds && !withSeeding ?
                new UnionFindResultProducer.Consecutive(dss, tracker) :
                new UnionFindResultProducer.Default(dss);
    }

    public static class ProcedureSetup {
        final Builder builder;
        final Graph graph;
        final AllocationTracker tracker;
        final ProcedureConfiguration procedureConfig;

        ProcedureSetup(
                final Builder builder,
                final Graph graph,
                final AllocationTracker tracker,
                final ProcedureConfiguration procedureConfig) {
            this.builder = builder;
            this.graph = graph;
            this.tracker = tracker;
            this.procedureConfig = procedureConfig;
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

    public static class StreamResult {

        /**
         * the mapped node id
         */
        public final long nodeId;

        /**
         * set id
         */
        public final long setId;

        public StreamResult(long nodeId, int setId) {
            this.nodeId = nodeId;
            this.setId = (long) setId;
        }

        public StreamResult(long nodeId, long setId) {
            this.nodeId = nodeId;
            this.setId = setId;
        }
    }

    public static class Builder extends AbstractCommunityResultBuilder<WriteResult> {
        private String partitionProperty;
        private String writeProperty;

        @Override
        protected WriteResult build(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount,
                long communityCount,
                Histogram communityHistogram,
                boolean write) {
            return new WriteResult(
                    loadMillis,
                    computeMillis,
                    postProcessingMillis,
                    writeMillis,
                    nodeCount,
                    communityCount,
                    communityHistogram.getValueAtPercentile(100),
                    communityHistogram.getValueAtPercentile(99),
                    communityHistogram.getValueAtPercentile(95),
                    communityHistogram.getValueAtPercentile(90),
                    communityHistogram.getValueAtPercentile(75),
                    communityHistogram.getValueAtPercentile(50),
                    communityHistogram.getValueAtPercentile(25),
                    communityHistogram.getValueAtPercentile(10),
                    communityHistogram.getValueAtPercentile(5),
                    communityHistogram.getValueAtPercentile(1),
                    write,
                    partitionProperty,
                    writeProperty
            );
        }

        public Builder withPartitionProperty(String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        public Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }

    public interface UnionFindResultProducer {

        /**
         * Computes the set id of a given ID.
         *
         * @param p an id
         * @return corresponding set id
         */
        long setIdOf(long p);

        /**
         * Computes the result stream based on a given ID mapping by using
         * {@link #setIdOf(long)} to look up the set representative for each node id.
         *
         * @param idMapping mapping between internal ids and Neo4j ids
         * @return tuples of Neo4j ids and their set ids
         */
        default Stream<StreamResult> resultStream(IdMapping idMapping) {
            return LongStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
                    .mapToObj(mappedId -> new StreamResult(
                            idMapping.toOriginalNodeId(mappedId),
                            setIdOf(mappedId)));
        }

        class Default implements UnionFindResultProducer {

            private final DisjointSetStruct dss;

            Default(final DisjointSetStruct dss) {
                this.dss = dss;
            }

            @Override
            public long setIdOf(final long p) {
                return dss.setIdOf(p);
            }

        }

        class Consecutive implements UnionFindResultProducer {

            private final HugeLongArray communities;

            Consecutive(DisjointSetStruct dss, AllocationTracker tracker) {
                long nextConsecutiveId = -1L;

                // TODO is there a better way to set the initial size, e.g. dss.setCount
                HugeLongLongMap setIdToConsecutiveId = new HugeLongLongMap(dss.size() / 10, tracker);
                this.communities = HugeLongArray.newArray(dss.size(), tracker);

                for (int nodeId = 0; nodeId < dss.size(); nodeId++) {
                    long setId = dss.setIdOf(nodeId);
                    final long successiveId = setIdToConsecutiveId.getOrDefault(setId, -1);
                    if (successiveId == -1) {
                        setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                    }
                    communities.set(nodeId, nextConsecutiveId);
                }
            }

            @Override
            public long setIdOf(final long p) {
                return communities.get(p);
            }
        }

        /**
         * Responsible for writing back the set ids to Neo4j.
         */
        final class Translator implements PropertyTranslator.OfLong<UnionFindResultProducer> {

            public static final PropertyTranslator<UnionFindResultProducer> INSTANCE = new Translator();

            @Override
            public long toLong(final UnionFindResultProducer data, final long nodeId) {
                return data.setIdOf(nodeId);
            }
        }
    }
}
