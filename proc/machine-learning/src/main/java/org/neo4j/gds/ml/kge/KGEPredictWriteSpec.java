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
package org.neo4j.gds.ml.kge;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.AlgorithmSpecProgressTrackerProvider;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.nodesim.TopKGraph;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;

@GdsCallable(
    name = "gds.ml.kge.predict.write",
    description = "Predicts new relationships using an existing KGE model",
    executionMode = WRITE_RELATIONSHIP
)
public class KGEPredictWriteSpec implements AlgorithmSpec<
    TopKMapComputer,
    KGEPredictResult,
    KGEPredictWriteConfig,
    Stream<KGEWriteResult>,
    KGEPredictAlgorithmFactory<KGEPredictWriteConfig>> {
    @Override
    public String name() {
        return "KGEPredictWrite";
    }

    @Override
    public KGEPredictAlgorithmFactory<KGEPredictWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KGEPredictAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KGEPredictWriteConfig> newConfigFunction() {
        return (__, config) -> KGEPredictWriteConfig.of(config);
    }

    public Graph build(Stream<SimilarityResult> stream) {
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap.rootIdMap())
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of("property"))
            .concurrency(concurrency)
            .executorService(executorService)
            .build();

        ParallelUtil.parallelStreamConsume(
            stream,
            concurrency,
            terminationFlag,
            similarityStream -> similarityStream.forEach(similarityResult -> relationshipsBuilder.addFromInternal(
                idMap.toRootNodeId(similarityResult.sourceNodeId()),
                idMap.toRootNodeId(similarityResult.targetNodeId()),
                similarityResult.similarity
            ))
        );

        return GraphFactory.create(
            idMap.rootIdMap(),
            relationshipsBuilder.build()
        );
    }

    @Override
    public ComputationResultConsumer<TopKMapComputer, KGEPredictResult, KGEPredictWriteConfig, Stream<KGEWriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            KGEWriteResult.Builder builder = new KGEWriteResult.Builder();

            if (computationResult.result().isEmpty()) {
                return Stream.of(builder.build());
            }

            Graph graph = computationResult.graph();
            Prim prim = computationResult.algorithm();
            SpanningTree spanningTree = computationResult.result().get();
            SpanningTreeWriteConfig config = computationResult.config();

            var topKMap = computationResult
            var newRelGraph = new TopKGraph(graph, topKMap);

            try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {

                var spanningGraph = new SpanningGraph(graph, spanningTree);

                executionContext.relationshipExporterBuilder()
                    .withGraph(spanningGraph)
                    .withIdMappingOperator(spanningGraph::toOriginalNodeId)
                    .withTerminationFlag(prim.getTerminationFlag())
                    .withProgressTracker(AlgorithmSpecProgressTrackerProvider.createProgressTracker(
                        name(),
                        graph.nodeCount(),
                        config.writeConcurrency(),
                        executionContext
                    ))
                    .withArrowConnectionInfo(config.arrowConnectionInfo(), computationResult.graphStore().databaseId().databaseName())
                    .build()
                    .write(config.writeRelationshipType(), config.writeProperty());
            }
            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            builder.withRelationshipsWritten(spanningTree.effectiveNodeCount() - 1);
            builder.withConfig(config);
            return Stream.of(builder.build());
        };
    }
    }

}
