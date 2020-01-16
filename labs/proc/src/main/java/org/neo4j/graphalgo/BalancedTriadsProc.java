/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.triangle.BalancedTriadsConfig;
import org.neo4j.graphalgo.impl.triangle.BalancedTriads;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class BalancedTriadsProc extends TriangleBaseProc<BalancedTriads, BalancedTriads, BalancedTriadsConfig> {

    @Procedure(name = "gds.alpha.balancedTriads.stream", mode = READ)
    public Stream<BalancedTriads.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<BalancedTriads, BalancedTriads, BalancedTriadsConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        Graph graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return computationResult.algorithm().computeStream();
    }

    @Procedure(value = "gds.alpha.balancedTriads.write", mode = Mode.WRITE)
    public Stream<Result> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
    {
        ComputationResult<BalancedTriads, BalancedTriads, BalancedTriadsConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        Graph graph = computationResult.graph();
        BalancedTriads algorithm = computationResult.algorithm();
        BalancedTriadsConfig config = computationResult.config();
        AllocationTracker tracker = computationResult.tracker();

        BalancedTriadsResultBuilder builder = new BalancedTriadsResultBuilder(true, true, tracker);
        builder.setLoadMillis(computationResult.createMillis());
        builder.setComputeMillis(computationResult.computeMillis());
        builder.withBalancedProperty(config.balancedProperty());
        builder.withUnbalancedProperty(config.unbalancedProperty());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.buildResult());
        }

        try (ProgressTimer ignored = builder.timeWrite()) {
            NodePropertyExporter.of(api, graph, algorithm.terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build()
                .write(
                    config.balancedProperty(),
                    algorithm.getBalancedTriangles(),
                    PagedAtomicIntegerArray.Translator.INSTANCE,
                    config.unbalancedProperty(),
                    algorithm.getUnbalancedTriangles(),
                    PagedAtomicIntegerArray.Translator.INSTANCE);
        }

        return Stream.of(builder
            .withBalancedTriadCount(algorithm.getBalancedTriangleCount())
            .withUnbalancedTriadCount(algorithm.getUnbalancedTriangleCount())
            .withCommunityFunction(algorithm.getBalancedTriangles()::get)
            .build()
        );
    }

    @Override
    protected BalancedTriadsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return BalancedTriadsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<BalancedTriads, BalancedTriadsConfig> algorithmFactory(BalancedTriadsConfig config) {
        return new AlphaAlgorithmFactory<BalancedTriads, BalancedTriadsConfig>() {
            @Override
            public BalancedTriads build(
                Graph graph,
                BalancedTriadsConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new BalancedTriads(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    AllocationTracker.create()
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "BalancedTriads"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }

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

        public final String balancedProperty;
        public final String unbalancedProperty;

        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long postProcessingMillis,
                long nodeCount, long balancedTriadCount,
                long unbalancedTriadCount,
                long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p5, long p1,
                String balancedProperty,
                String unbalancedProperty) {
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
            this.balancedProperty = balancedProperty;
            this.unbalancedProperty = unbalancedProperty;
        }
    }

    public static class BalancedTriadsResultBuilder extends AbstractCommunityResultBuilder<Result> {

        private long balancedTriadCount = 0;
        private long unbalancedTriadCount = 0;
        private String balancedProperty;
        private String unbalancedProperty;

        BalancedTriadsResultBuilder(
            boolean buildHistogram,
            boolean buildCommunityCount,
            AllocationTracker tracker
        ) {
            super(buildHistogram, buildCommunityCount, tracker);
        }


        BalancedTriadsResultBuilder withBalancedTriadCount(long balancedTriadCount) {
            this.balancedTriadCount = balancedTriadCount;
            return this;
        }

        BalancedTriadsResultBuilder withBalancedProperty(String property) {
            this.balancedProperty = property;
            return this;
        }

        BalancedTriadsResultBuilder withUnbalancedProperty(String property) {
            this.unbalancedProperty = property;
            return this;
        }

        BalancedTriadsResultBuilder withUnbalancedTriadCount(long unbalancedTriadCount) {
            this.unbalancedTriadCount = unbalancedTriadCount;
            return this;
        }

        @Override
        protected Result buildResult() {
            return new Result(
                loadMillis, computeMillis, writeMillis, postProcessingDuration, nodeCount, balancedTriadCount, unbalancedTriadCount,
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(100)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(99)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(95)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(90)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(75)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(50)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(25)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(10)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(5)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(1)).orElse(0L),
                balancedProperty,
                unbalancedProperty
            );
        }
    }

}
