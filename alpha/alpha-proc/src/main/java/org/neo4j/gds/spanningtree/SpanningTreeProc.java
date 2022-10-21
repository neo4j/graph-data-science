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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.impl.spanningtree.Prim;
import org.neo4j.gds.impl.spanningtree.SpanningGraph;
import org.neo4j.gds.impl.spanningtree.SpanningTree;
import org.neo4j.gds.impl.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.impl.spanningtree.SpanningTreeConfig;
import org.neo4j.procedure.Context;

import java.util.stream.Stream;

// TODO: Always undirected
public abstract class SpanningTreeProc extends AlgoBaseProc<Prim, SpanningTree, SpanningTreeConfig, Prim.Result> {

    @Context
    public RelationshipExporterBuilder<? extends RelationshipExporter> relationshipExporterBuilder;

    @Override
    public GraphAlgorithmFactory<Prim, SpanningTreeConfig> algorithmFactory() {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeConfig, Stream<Prim.Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            Prim prim = computationResult.algorithm();
            SpanningTree spanningTree = computationResult.result();
            SpanningTreeConfig config = computationResult.config();

            Prim.Builder builder = new Prim.Builder();

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(builder.build());
            }

            builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount());
            try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {

                var spanningGraph = new SpanningGraph(graph, spanningTree);
                var progressTracker = new TaskProgressTracker(
                    RelationshipExporter.baseTask("SpanningTree", graph.relationshipCount()),
                    log,
                    RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
                    taskRegistryFactory
                );

                relationshipExporterBuilder
                    .withGraph(spanningGraph)
                    .withIdMappingOperator(spanningGraph::toOriginalNodeId)
                    .withTerminationFlag(prim.getTerminationFlag())
                    .withProgressTracker(progressTracker)
                    .build()
                    .write(config.writeProperty(), config.weightWriteProperty());
            }
            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            return Stream.of(builder.build());
        };
    }
}
