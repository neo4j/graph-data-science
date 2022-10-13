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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.impl.spanningTree.KSpanningTree;
import org.neo4j.gds.impl.spanningTree.KSpanningTreeConfig;
import org.neo4j.gds.impl.spanningTree.Prim;
import org.neo4j.gds.impl.spanningTree.SpanningTree;
import org.neo4j.gds.utils.InputNodeValidator;

import java.util.stream.Stream;

public abstract class KSpanningTreeProc extends NodePropertiesWriter<KSpanningTree, SpanningTree, KSpanningTreeConfig, Prim.Result> {


    @Override
    public GraphAlgorithmFactory<KSpanningTree, KSpanningTreeConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "KSpanningTree";
            }

            @Override
            public Task progressTask(
                Graph graph, KSpanningTreeConfig config
            ) {
                return Tasks.task(
                    taskName(),
                    Tasks.leaf("SpanningTree", graph.nodeCount()),
                    Tasks.leaf("Add relationship weights"),
                    Tasks.leaf("Remove relationships")
                );
            }

            @Override
            public KSpanningTree build(
                Graph graph,
                KSpanningTreeConfig configuration,
                ProgressTracker progressTracker
            ) {
                InputNodeValidator.validateStartNode(configuration.startNodeId(), graph);
                return new KSpanningTree(
                    graph,
                    graph,
                    graph,
                    configuration.minMax(),
                    configuration.startNodeId(),
                    configuration.k(),
                    progressTracker
                );
            }
        };
    }

    @Override
    public ComputationResultConsumer<KSpanningTree, SpanningTree, KSpanningTreeConfig, Stream<Prim.Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            SpanningTree spanningTree = computationResult.result();
            KSpanningTreeConfig config = computationResult.config();

            Prim.Builder builder = new Prim.Builder();

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(builder.build());
            }

            builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount);
            try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
                var progressTracker = new TaskProgressTracker(
                    NodePropertyExporter.baseTask("KSpanningTree", graph.nodeCount()),
                    log,
                    config.writeConcurrency(),
                    taskRegistryFactory
                );
                final NodePropertyExporter exporter = nodePropertyExporterBuilder
                    .withIdMap(graph)
                    .withTerminationFlag(TerminationFlag.wrap(transaction)).withProgressTracker(progressTracker)
                    .parallel(Pools.DEFAULT, config.writeConcurrency())
                    .build();

                var properties = new DoubleNodePropertyValues() {
                    @Override
                    public long size() {
                        return computationResult.graph().nodeCount();
                    }

                    @Override
                    public double doubleValue(long nodeId) {
                        return spanningTree.head((int) nodeId);
                    }
                };

                exporter.write(
                    config.writeProperty(),
                    properties
                );

                builder.withNodePropertiesWritten(exporter.propertiesWritten());
            }

            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            return Stream.of(builder.build());
        };
    }
}
