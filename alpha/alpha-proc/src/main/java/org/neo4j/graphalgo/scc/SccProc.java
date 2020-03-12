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
package org.neo4j.graphalgo.scc;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.impl.scc.SccConfig;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.scc.SccAlgorithm;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class SccProc extends AlgoBaseProc<SccAlgorithm, HugeLongArray, SccConfig> {

    private static final String DESCRIPTION =
        "The SCC algorithm finds sets of connected nodes in an directed graph, " +
        "where all nodes in the same set form a connected component.";

    @Procedure(value = "gds.alpha.scc.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<SccResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<SccAlgorithm, HugeLongArray, SccConfig> computationResult = compute(graphNameOrConfig, configuration);

        SccAlgorithm algorithm = computationResult.algorithm();
        HugeLongArray components = computationResult.result();
        SccConfig config = computationResult.config();
        AllocationTracker tracker = computationResult.tracker();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<SccResult> writeBuilder = new SccResultBuilder(
            graph.nodeCount(),
            callContext,
            computationResult.tracker()
        )
            .withBuildCommunityCount(true)
            .withBuildHistogram(true)
            .withWriteProperty(config.writeProperty())
            .withCommunityFunction(components != null ? components::get : null)
            .withCreateMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(writeBuilder.build());
        }

        log.info("Scc: overall memory usage: %s", tracker.getUsageString());

        try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();
            exporter
                .write(
                    config.writeProperty(),
                    components,
                    HugeLongArray.Translator.INSTANCE
                );

            writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }

        graph.release();
        return Stream.of(writeBuilder.build());
    }

    @Procedure(value = "gds.alpha.scc.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SccAlgorithm.StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<SccAlgorithm, HugeLongArray, SccConfig> computationResult = compute(graphNameOrConfig, configuration);

        AllocationTracker tracker = computationResult.tracker();
        Graph graph = computationResult.graph();
        HugeLongArray components = computationResult.result();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        log.info("Scc: overall memory usage: %s", tracker.getUsageString());
        graph.release();

        return LongStream.range(0, graph.nodeCount())
                .filter(i -> components.get(i) != -1)
                .mapToObj(i -> new SccAlgorithm.StreamResult(graph.toOriginalNodeId(i), components.get(i)));
    }

    @Override
    protected SccConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return SccConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<SccAlgorithm, SccConfig> algorithmFactory(SccConfig config) {
        return new AlphaAlgorithmFactory<SccAlgorithm, SccConfig>() {
            @Override
            public SccAlgorithm build(
                Graph graph, SccConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new SccAlgorithm(graph, tracker)
                    .withProgressLogger(ProgressLogger.wrap(log, "Scc"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }

    public static class SccResult {

        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final long communityCount;
        public final long setCount;
        public final long minSetSize;
        public final long maxSetSize;
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
        public final String writeProperty;

        public SccResult(
            long createMillis,
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
            long minSetSize,
            long maxSetSize,
            String writeProperty
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.setCount = this.communityCount = communityCount;
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
            this.minSetSize = minSetSize;
            this.maxSetSize = maxSetSize;
            this.writeProperty = writeProperty;
        }
    }

    public static final class SccResultBuilder extends AbstractCommunityResultBuilder<SccResult> {
        private String writeProperty;

        SccResultBuilder(
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(nodeCount, context, tracker);
        }

        @Override
        public SccResult buildResult() {
            return new SccResult(
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                nodeCount,
                maybeCommunityCount.orElse(0),
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
                maybeCommunityHistogram.map(h -> h.getMinNonZeroValue()).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getMaxValue()).orElse(0L),
                writeProperty
            );
        }

        public SccResultBuilder withBuildHistogram(boolean buildHistogram) {
            this.buildHistogram = buildHistogram;
            return this;
        }

        public SccResultBuilder withBuildCommunityCount(boolean buildCommunityCount) {
            this.buildCommunityCount = buildCommunityCount;
            return this;
        }


        public SccResultBuilder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }
}
