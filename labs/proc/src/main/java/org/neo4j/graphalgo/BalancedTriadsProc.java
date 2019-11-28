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
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.triangle.BalancedTriads;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class BalancedTriadsProc extends LabsProc {

    public static final String DEFAULT_BALANCED_PROPERTY = "balanced";
    public static final String DEFAULT_UNBALANCED_PROPERTY = "unbalanced";

    @Procedure(name = "algo.balancedTriads.stream", mode = READ)
    @Description("CALL algo.balancedTriads.stream(label, relationship, {concurrency:8}) " +
            "YIELD nodeId, balanced, unbalanced")
    public Stream<BalancedTriads.Result> balancedTriadsStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        // load
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipProperties(PropertyMapping.of(configuration.getWeightProperty(), 0.0))
                .withLog(log)
                .sorted()
                .undirected()
                .load(configuration.getGraphImpl(HugeGraph.TYPE, HugeGraph.TYPE));

        // omit empty graphs
        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        // compute
        return new BalancedTriads(graph, Pools.DEFAULT, configuration.concurrency(), AllocationTracker.create())
                .withProgressLogger(ProgressLogger.wrap(log, "balancedTriads"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .stream();
    }


    @Procedure(value = "algo.balancedTriads", mode = Mode.WRITE)
    @Description("CALL algo.balancedTriads(label, relationship" +
            "{concurrency:4, write:true, weightProperty:'w', balancedProperty:'balanced', unbalancedProperty:'unbalanced'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, balancedTriadCount, unbalancedTriadCount")
    public Stream<Result> balancedTriads(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final BalancedTriads balancedTriads;

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        final BalancedTriadsResultBuilder builder = new BalancedTriadsResultBuilder();

        // load
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withRelationshipProperties(PropertyMapping.of(configuration.getWeightProperty(), 0.0))
                    .withLog(log)
                    .sorted()
                    .undirected()
                    .load(configuration.getGraphImpl(HugeGraph.TYPE, HugeGraph.TYPE));
        }

        // compute
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        try (ProgressTimer timer = builder.timeEval()) {
            balancedTriads = new BalancedTriads(graph, Pools.DEFAULT, configuration.concurrency(), AllocationTracker.create())
                    .withProgressLogger(ProgressLogger.wrap(log, "balancedTriads"))
                    .withTerminationFlag(terminationFlag)
                    .compute();
        }

        // write
        if (configuration.isWriteFlag()) {
            builder.withWrite(true);

            String balancedProperty = configuration.get("balancedProperty", DEFAULT_BALANCED_PROPERTY);
            builder.withBalancedProperty(balancedProperty);

            String unbalancedProperty = configuration.get("unbalancedProperty", DEFAULT_UNBALANCED_PROPERTY);
            builder.withUnbalancedProperty(unbalancedProperty);

            try (ProgressTimer timer = builder.timeWrite()) {
                NodePropertyExporter.of(api, graph, balancedTriads.terminationFlag)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                        .build()
                        .write(
                                balancedProperty,
                                balancedTriads.getBalancedTriangles(),
                                PagedAtomicIntegerArray.Translator.INSTANCE,
                                unbalancedProperty,
                                balancedTriads.getUnbalancedTriangles(),
                                PagedAtomicIntegerArray.Translator.INSTANCE);
            }
        }

        // result
        builder.withBalancedTriadCount(balancedTriads.getBalancedTriangleCount())
                .withUnbalancedTriadCount(balancedTriads.getUnbalancedTriangleCount());
        return Stream.of(builder.buildfromKnownLongSizes(graph.nodeCount(), balancedTriads.getBalancedTriangles()::get));
    }

    /**
     * result dto
     */
    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;

        public final long nodeCount;
        public final long balancedTriadCount;
        public final long unbalancedTriadCount;

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
        public final String balancedProperty;
        public final String unbalancedProperty;


        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount, long balancedTriadCount,
                long unbalancedTriadCount,
                long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p5, long p1, boolean write, String balancedProperty, String unbalancedProperty) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodeCount = nodeCount;
            this.balancedTriadCount = balancedTriadCount;
            this.unbalancedTriadCount = unbalancedTriadCount;
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
            this.balancedProperty = balancedProperty;
            this.unbalancedProperty = unbalancedProperty;
        }
    }

    public class BalancedTriadsResultBuilder extends AbstractCommunityResultBuilder<Result> {

        private long balancedTriadCount = 0;
        private long unbalancedTriadCount = 0;
        private String balancedProperty;
        private String unbalancedProperty;


        public BalancedTriadsResultBuilder withBalancedTriadCount(long balancedTriadCount) {
            this.balancedTriadCount = balancedTriadCount;
            return this;
        }

        public BalancedTriadsResultBuilder withBalancedProperty(String property) {
            this.balancedProperty = property;
            return this;
        }

        public BalancedTriadsResultBuilder withUnbalancedProperty(String property) {
            this.unbalancedProperty = property;
            return this;
        }

        public BalancedTriadsResultBuilder withUnbalancedTriadCount(long unbalancedTriadCount) {
            this.unbalancedTriadCount = unbalancedTriadCount;
            return this;
        }

        @Override
        protected Result build(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount,
                long communityCount,
                Histogram communityHistogram,
                boolean write) {
            return new Result(
                    loadMillis, computeMillis, writeMillis, postProcessingMillis,  nodeCount, balancedTriadCount, unbalancedTriadCount,
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
                    balancedProperty,
                    unbalancedProperty
            );
        }
    }

}
