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
import org.neo4j.gds.api.ProcedureReturnColumns;
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
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.scc.SccProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.scc.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class SccWriteProc extends SccProc<WriteResult> {

    @Procedure(value = "gds.alpha.scc.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<SccAlgorithm, HugeLongArray, SccConfig, Stream<WriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            SccAlgorithm algorithm = computationResult.algorithm();
            HugeLongArray components = computationResult.result();
            SccConfig config = computationResult.config();
            Graph graph = computationResult.graph();

            AbstractResultBuilder<WriteResult> writeBuilder = new SccResultBuilder(
                executionContext.returnColumns(),
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
                return Stream.of(writeBuilder.build());
            }

            try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
                var progressTracker = new TaskProgressTracker(
                    NodePropertyExporter.baseTask("Scc", graph.nodeCount()),
                    executionContext.log(),
                    config.writeConcurrency(),
                    executionContext.taskRegistryFactory()
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

            return Stream.of(writeBuilder.build());
        };
    }

    public static final class SccResultBuilder extends AbstractCommunityResultBuilder<WriteResult> {

        SccResultBuilder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public WriteResult buildResult() {
            return new WriteResult(
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
