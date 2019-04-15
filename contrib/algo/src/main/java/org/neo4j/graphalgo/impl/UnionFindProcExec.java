/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class UnionFindProcExec implements BiConsumer<String, Algorithm<?>> {

    private static final String CONFIG_THRESHOLD = "threshold";
    private static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    private static final String CONFIG_OLD_CLUSTER_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";


    private final GraphDatabaseAPI api;
    private final Log log;
    private final KernelTransaction transaction;
    private final  UnionFindAlgo sequential;
    private final UnionFindAlgo parallel;

    public static Stream<UnionFindResult> run(
            Map<String, Object> config,
            String label,
            String relationship,
            Supplier<UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();

        final Builder builder = new Builder();

        UnionFindProcExec uf = unionFind.get();

        final Graph graph = uf.load(builder::timeLoad, configuration, tracker);

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(UnionFindResult.EMPTY);
        }

        final DSSResult dssResult = uf.evaluate(
                builder::timeEval,
                graph,
                configuration,
                tracker);
        graph.release();

        if (configuration.isWriteFlag()) {
            String writeProperty = configuration.get(CONFIG_CLUSTER_PROPERTY, CONFIG_OLD_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY);
            builder.withWrite(true);
            builder.withPartitionProperty(writeProperty).withWriteProperty(writeProperty);

            uf.write(builder::timeWrite, graph, dssResult, configuration, writeProperty);
        }

        if (dssResult.isHuge) {
            return Stream.of(builder.build(graph.nodeCount(), dssResult.hugeStruct::find));
        } else {
            return Stream.of(builder.build(graph.nodeCount(), l -> (long) dssResult.struct.find((int) l)));
        }
    }

    public static class UnionFindResult {

        public static final UnionFindProcExec.UnionFindResult EMPTY = new UnionFindProcExec.UnionFindResult(
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


        public UnionFindResult(long loadMillis, long computeMillis, long postProcessingMillis, long writeMillis, long nodes, long communityCount,
                               long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p5, long p1, boolean write,
                               String partitionProperty, String writeProperty) {
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
        protected UnionFindResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram, boolean write) {
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

    public static Stream<DisjointSetStruct.Result> stream(
            Map<String, Object> config,
            String label,
            String relationship,
            Supplier<UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();
        UnionFindProcExec uf = unionFind.get();

        final Graph graph = uf.load(configuration, tracker);

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        DSSResult result = uf.evaluate(graph, configuration, tracker);
        graph.release();
        return result.resultStream(graph);
    }

    public UnionFindProcExec(
            GraphDatabaseAPI api,
            Log log,
            KernelTransaction transaction,
            UnionFindAlgo sequential,
            UnionFindAlgo parallel) {
        this.api = api;
        this.log = log;
        this.transaction = transaction;
        this.sequential = sequential;
        this.parallel = parallel;
    }

    public Graph load(
            Supplier<ProgressTimer> timer,
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return load(config, tracker);
        }
    }

    public Graph load(
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        return new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withAllocationTracker(tracker)
                .load(config.getGraphImpl());
    }

    private DSSResult evaluate(
            Supplier<ProgressTimer> timer,
            Graph graph,
            ProcedureConfiguration config,
            final AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return evaluate(graph, config, tracker);
        }
    }

    private DSSResult evaluate(
            Graph graph,
            ProcedureConfiguration config,
            final AllocationTracker tracker) {
        int concurrency = config.getConcurrency();
        int minBatchSize = config.getBatchSize();
        final double threshold = config.get(CONFIG_THRESHOLD, Double.NaN);
        UnionFindAlgo uf = concurrency > 1 ? parallel : sequential;
        return uf.runAny(
                graph,
                Pools.DEFAULT,
                tracker,
                minBatchSize,
                concurrency,
                threshold,
                this);
    }

    private void write(
            Supplier<ProgressTimer> timer,
            Graph graph,
            DSSResult struct,
            ProcedureConfiguration configuration, String writeProperty) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration, writeProperty);
        }
    }

    private void write(
            Graph graph,
            DSSResult struct,
            ProcedureConfiguration configuration, String writeProperty) {
        log.debug("Writing results");
        Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(
                        Pools.DEFAULT,
                        configuration.getConcurrency(),
                        TerminationFlag.wrap(transaction))
                .build();
        if (struct.hugeStruct != null) {
            write(exporter, struct.hugeStruct, writeProperty);
        } else {
            write(exporter, struct.struct, writeProperty);
        }
    }

    @Override
    public void accept(final String name, final Algorithm<?> algorithm) {
        algorithm.withProgressLogger(ProgressLogger.wrap(log, name))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
    }

    private void write(
            Exporter exporter,
            DisjointSetStruct struct, String writeProperty) {
        exporter.write(
                writeProperty,
                struct,
                DisjointSetStruct.Translator.INSTANCE);
    }

    private void write(
            Exporter exporter,
            PagedDisjointSetStruct struct, String writeProperty) {
        exporter.write(
                writeProperty,
                struct,
                PagedDisjointSetStruct.Translator.INSTANCE);
    }



}
