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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.MSColoring;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class MSColoringProc {

    public static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public ProcedureCallContext callContext;

    @Procedure(value = "algo.unionFind.mscoloring", mode = Mode.WRITE)
    @Description("CALL algo.unionFind.mscoloring(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> mscoloring(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();
        WriteResultBuilder builder = new WriteResultBuilder(callContext.outputFields(), tracker);

        // loading
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = load(configuration, tracker);
        }
        builder.withNodeCount(graph.nodeCount());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(WriteResult.EMPTY);
        }

        // evaluation
        final AtomicIntegerArray struct;
        try (ProgressTimer timer = builder.timeEval()) {
            struct = evaluate(graph, configuration);
        }
        builder.withCommunityFunction(n -> (long) struct.get((int) n));

        if (configuration.isWriteFlag()) {
            // write back
            builder.timeWrite(() ->
                    write(graph, struct, configuration));
        }

        return Stream.of(builder.build());
    }

    @Procedure(name = "algo.unionFind.mscoloring.stream", mode = READ)
    @Description("CALL algo.unionFind.mscoloring.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<MSColoring.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        // loading
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(configuration, tracker);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }


        // evaluation
        return new MSColoring(graph, Pools.DEFAULT, configuration.getConcurrency())
                .compute()
                .resultStream();
    }

    private Graph load(
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        return new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .withAllocationTracker(tracker)
                .withRelationshipProperties(PropertyMapping.of(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0)))
                .withDirection(Direction.OUTGOING)
                .load(config.getGraphImpl());
    }

    private AtomicIntegerArray evaluate(Graph graph, ProcedureConfiguration config) {
        return new MSColoring(graph, Pools.DEFAULT, config.getConcurrency())
                .compute()
                .getColors();
    }

    private void write(Graph graph, AtomicIntegerArray struct, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        Exporter.of(api, graph)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getWriteConcurrency(), null)
                .build()
                .write(
                        configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                        struct,
                        Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR
                );
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
