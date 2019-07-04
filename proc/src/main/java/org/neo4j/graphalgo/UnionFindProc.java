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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphalgo.impl.unionfind.GraphUnionFindAlgo;
import org.neo4j.graphalgo.impl.unionfind.UnionFindAlgorithmType;
import org.neo4j.graphalgo.impl.unionfind.UnionFindFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.COMMUNITY_TYPE;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_COMMUNITY_PROPERTY;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindFactory.CONFIG_PARALLEL_ALGO;

/**
 * @author mknblch
 */
public class UnionFindProc<T extends GraphUnionFindAlgo<T>> extends BaseAlgoProc<T> {

    private static final String CONFIG_THRESHOLD = "threshold";

    private static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    private static final String CONFIG_OLD_CLUSTER_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Procedure(value = "algo.unionFind", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{weightProperty:'weight', threshold:0.42, defaultValue:1.0, write: true, writeProperty:'community', communityProperty:'oldCommunity'}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindAlgorithmType.QUEUE);
    }

    @Procedure(value = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{weightProperty:'propertyName', threshold:0.42, defaultValue:1.0) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindAlgorithmType.QUEUE);
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

    @Procedure(value = "algo.unionFind.queue", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition',concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFindQueue(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindAlgorithmType.QUEUE);
    }

    @Procedure(value = "algo.unionFind.queue.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindQueueStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindAlgorithmType.QUEUE);
    }

    @Procedure(value = "algo.unionFind.forkJoinMerge", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFindForkJoinMerge(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindAlgorithmType.FJ_MERGE);
    }

    @Procedure(value = "algo.unionFind.forkJoinMerge.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindForkJoinMergeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindAlgorithmType.FJ_MERGE);
    }

    @Procedure(value = "algo.unionFind.forkJoin", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition',concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFindForkJoin(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindAlgorithmType.FORK_JOIN);
    }

    @Procedure(value = "algo.unionFind.forkJoin.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0,concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindForJoinStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, UnionFindAlgorithmType.FORK_JOIN);
    }

    public Stream<DisjointSetStruct.Result> stream(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindAlgorithmType algoImpl) {

        final Builder builder = new Builder();

        config.put(CONFIG_PARALLEL_ALGO, algoImpl.name());

        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        Graph graph = this.loadGraph(configuration, tracker, builder);
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        DisjointSetStruct communities = compute(builder, tracker, configuration, graph);
        graph.release();
        return communities.resultStream(graph);
    }

    private Stream<UnionFindResult> run(
            String label,
            String relationship,
            Map<String, Object> config,
            UnionFindAlgorithmType algoImpl) {

        final Builder builder = new Builder();

        config.put(CONFIG_PARALLEL_ALGO, algoImpl.name());

        ProcedureConfiguration configuration = newConfig(label, relationship, config);

        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = this.loadGraph(configuration, tracker, builder);
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(UnionFindResult.EMPTY);
        }

        DisjointSetStruct communities = compute(builder, tracker, configuration, graph);

        if (configuration.isWriteFlag()) {
            String writeProperty = configuration.get(
                    CONFIG_CLUSTER_PROPERTY,
                    CONFIG_OLD_CLUSTER_PROPERTY,
                    DEFAULT_CLUSTER_PROPERTY);
            builder.withWrite(true);
            builder.withPartitionProperty(writeProperty).withWriteProperty(writeProperty);

            write(builder::timeWrite, graph, communities, configuration, writeProperty);
        }

        return Stream.of(builder.build(tracker, graph.nodeCount(), communities::find));
    }

    private PropertyMapping[] createPropertyMappings(String communityProperty) {
        return new PropertyMapping[]{
                PropertyMapping.of(COMMUNITY_TYPE, communityProperty, -1),
        };
    }

    private void write(
            Supplier<ProgressTimer> timer,
            Graph graph,
            DisjointSetStruct struct,
            ProcedureConfiguration configuration, String writeProperty) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration, writeProperty);
        }
    }

    private void write(
            Graph graph,
            DisjointSetStruct struct,
            ProcedureConfiguration configuration, String writeProperty) {
        log.debug("Writing results");
        Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(
                        Pools.DEFAULT,
                        configuration.getWriteConcurrency(),
                        TerminationFlag.wrap(transaction))
                .build();
        exporter.write(
                writeProperty,
                struct,
                DisjointSetStruct.Translator.INSTANCE);
    }

    private DisjointSetStruct compute(
            final Builder builder,
            final AllocationTracker tracker,
            final ProcedureConfiguration configuration,
            final Graph graph) {

        T algo = newAlgorithm(graph, configuration, tracker);
        final DisjointSetStruct algoResult = runWithExceptionLogging(
                "UnionFind failed",
                () -> builder.timeEval((Supplier<DisjointSetStruct>) algo::compute));

        log.info("UnionFind: overall memory usage: %s", tracker.getUsageString());

        algo.release();
        graph.release();

        return algoResult;
    }

    @Override
    GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {

        final String communityProperty = config.getString(CONFIG_COMMUNITY_PROPERTY, null);

        if (communityProperty != null) {
            loader.withOptionalNodeProperties(createPropertyMappings(communityProperty));
        }

        return loader
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING);
    }

    @Override
    UnionFindFactory<T> algorithmFactory(final ProcedureConfiguration config) {
        double threshold = config.get(CONFIG_THRESHOLD, Double.NaN);
        String algoName = config.getString(CONFIG_PARALLEL_ALGO, UnionFindAlgorithmType.QUEUE.name());

        UnionFindAlgorithmType algorithmType = null;

        if (config.isSingleThreaded()) {
            algorithmType = UnionFindAlgorithmType.SEQ;
        } else {
            for (final UnionFindAlgorithmType algoType : UnionFindAlgorithmType.values()) {
                if (algoType.name().equalsIgnoreCase(algoName)) {
                    algorithmType = algoType;
                }
            }
            if (algorithmType == null) {
                String errorMsg = String.format("Parallel configuration %s is invalid. Valid names are %s", algoName,
                        Arrays.stream(UnionFindAlgorithmType.values())
                                .filter(ufa -> ufa != UnionFindAlgorithmType.SEQ)
                                .map(UnionFindAlgorithmType::name)
                                .collect(Collectors.joining(", ")));
                throw new IllegalArgumentException(errorMsg);
            }
        }
        return new UnionFindFactory<>(algorithmType, threshold);
    }

    public static class UnionFindResult {

        public static final UnionFindResult EMPTY = new UnionFindResult(
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


        public UnionFindResult(
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

    public static class Builder extends AbstractCommunityResultBuilder<UnionFindResult> {
        private String partitionProperty;
        private String writeProperty;

        @Override
        protected UnionFindResult build(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount,
                long communityCount,
                Histogram communityHistogram,
                boolean write) {
            return new UnionFindResult(
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
}
