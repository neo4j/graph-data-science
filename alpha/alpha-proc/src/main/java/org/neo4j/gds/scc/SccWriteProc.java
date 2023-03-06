/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.scc;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.scc.SccAlgorithm;
import org.neo4j.gds.impl.scc.SccConfig;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.scc.SccProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.scc.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class SccWriteProc extends SccProc<SccWriteProc.SccResult> {

    @Procedure(value = "gds.alpha.scc.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<SccResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<SccAlgorithm, HugeLongArray, SccConfig, Stream<SccResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            SccAlgorithm algorithm = computationResult.algorithm();
            HugeLongArray components = computationResult.result();
            SccConfig config = computationResult.config();
            Graph graph = computationResult.graph();

            AbstractResultBuilder<SccResult> writeBuilder = new SccResultBuilder(
                callContext,
                config.concurrency()
            )
                .buildCommunityCount(true)
                .buildHistogram(true)
                .withCommunityFunction(components != null ? components::get : null)
                .withNodeCount(graph.nodeCount())
                .withConfig(config)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis());

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(writeBuilder.build());
            }

            try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
                var progressTracker = new TaskProgressTracker(
                    NodePropertyExporter.baseTask("Scc", graph.nodeCount()),
                    log,
                    config.writeConcurrency(),
                    taskRegistryFactory
                );
                NodePropertyExporter exporter = nodePropertyExporterBuilder
                    .withIdMap(graph)
                    .withTerminationFlag(algorithm.getTerminationFlag())
                    .withProgressTracker(progressTracker)
                    .parallel(Pools.DEFAULT, config.writeConcurrency())
                    .build();

                var properties = new LongNodePropertyValues() {
                    @Override
                    public long nodeCount() {
                        return computationResult.graph().nodeCount();
                    }

                    @Override
                    public long longValue(long nodeId) {
                        return components.get(nodeId);
                    }
                };

                exporter.write(
                    config.writeProperty(),
                    properties
                );

                writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
            }

            graph.release();
            return Stream.of(writeBuilder.build());
        };
    }

    @SuppressWarnings("unused")
    public static class SccResult {

        public final long preProcessingMillis;
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
            long preProcessingMillis,
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
            this.preProcessingMillis = preProcessingMillis;
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

        SccResultBuilder(ProcedureCallContext context, int concurrency) {
            super(context, concurrency);
        }

        @Override
        public SccResult buildResult() {
            return new SccResult(
                preProcessingMillis,
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
                config instanceof WritePropertyConfig ? ((WritePropertyConfig) config).writeProperty() : ""
            );
        }

        public SccResultBuilder buildHistogram(boolean buildHistogram) {
            this.buildHistogram = buildHistogram;
            return this;
        }

        public SccResultBuilder buildCommunityCount(boolean buildCommunityCount) {
            this.buildCommunityCount = buildCommunityCount;
            return this;
        }
    }
}
