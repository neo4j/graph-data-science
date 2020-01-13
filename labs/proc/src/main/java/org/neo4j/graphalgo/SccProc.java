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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.scc.SccAlgorithm;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class SccProc extends AlgoBaseProc<SccAlgorithm, HugeLongArray, SccConfig> {

    private static final String SCC_DESCRIPTION =
        "The SCC algorithm finds sets of connected nodes in an directed graph, where all nodes in the same set form a connected component.";

    @Procedure(value = "gds.alpha.scc.write", mode = Mode.WRITE)
    @Description(SCC_DESCRIPTION)
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

        AbstractResultBuilder<SccResult> builder = new SccResultBuilder(true, true, tracker)
            .withCommunityFunction(components::get)
            .withLoadMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withWriteProperty(config.writeProperty())
            .withNodeCount(graph.nodeCount());

        if (graph.isEmpty()) {
            return Stream.of(builder.build());
        }

        log.info("Scc: overall memory usage: %s", tracker.getUsageString());

        try (ProgressTimer ignored = builder.timeWrite()) {
            NodePropertyExporter.of(api, graph, algorithm.terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build()
                .write(
                    config.writeProperty(),
                    components,
                    HugeLongArray.Translator.INSTANCE
                );
        }

        return Stream.of(builder.build());
    }

    // default algo.scc -> iter tarjan
    @Procedure(value = "algo.scc.stream", mode = READ)
    @Description("CALL algo.scc.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
                 "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SccAlgorithm.StreamResult> sccDefaultMethodStream(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationship,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {

        return sccIterativeTarjanStream(label, relationship, config);
    }

    private Stream<SccAlgorithm.StreamResult> sccIterativeTarjanStream(
        String label,
        String relationship,
        Map<String, Object> config
    ) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
            .init(log, label, relationship, configuration)
            .withDirection(Direction.OUTGOING)
            .load(configuration.getGraphImpl());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        final AllocationTracker tracker = AllocationTracker.create();
        final SccAlgorithm algo = SccAlgorithm.iterativeTarjan(graph, tracker)
            .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
            .withTerminationFlag(TerminationFlag.wrap(transaction));
        algo.compute();

        graph.release();

        return algo.resultStream();
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
                return SccAlgorithm.iterativeTarjan(graph, tracker)
                    .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }

    public static class SccResult {

        public final long loadMillis;
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
            long minSetSize,
            long maxSetSize,
            String writeProperty
        ) {
            this.loadMillis = loadMillis;
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

    static final class SccResultBuilder extends AbstractCommunityResultBuilder<SccResult> {
        private String writeProperty;

        public SccResultBuilder(boolean buildHistogram, boolean buildCommunityCount, AllocationTracker tracker) {
            super(buildHistogram, buildCommunityCount, tracker);
        }

        @Override
        public SccResult buildResult() {
            return new SccResult(
                loadMillis,
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

        public SccResultBuilder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }
}
