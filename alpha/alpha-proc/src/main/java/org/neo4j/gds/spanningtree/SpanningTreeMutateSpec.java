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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.spanningtree.Prim;
import org.neo4j.gds.impl.spanningtree.SpanningGraph;
import org.neo4j.gds.impl.spanningtree.SpanningTree;
import org.neo4j.gds.impl.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.impl.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.values.storable.NumberType;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.alpha.spanningTree.mutate", description = SpanningTreeWriteProc.DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class SpanningTreeMutateSpec implements AlgorithmSpec<Prim, SpanningTree, SpanningTreeMutateConfig, Stream<MutateResult>, SpanningTreeAlgorithmFactory<SpanningTreeMutateConfig>> {

    @Override
    public String name() {
        return "SpanningTreeMutate";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeMutateConfig> algorithmFactory() {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeMutateConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeMutateConfig.of(config);

    }

    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeMutateConfig, Stream<MutateResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            Prim prim = computationResult.algorithm();
            SpanningTree spanningTree = computationResult.result();
            SpanningTreeMutateConfig config = computationResult.config();

            MutateResult.Builder builder = new MutateResult.Builder();

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(builder.build());
            }

            var relationshipsBuilder = GraphFactory
                .initRelationshipsBuilder()
                .nodes(computationResult.graph())
                .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
                .orientation(Orientation.NATURAL)
                .build();

            var mutateRelationshipType = RelationshipType.of(config.mutateProperty());

            builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount());
            builder.withTotalWeight(spanningTree.totalWeight());

            Relationships relationships;

            try (ProgressTimer ignored = ProgressTimer.start(builder::withMutateMillis)) {

                var spanningGraph = new SpanningGraph(graph, spanningTree);
                spanningGraph.forEachNode(nodeId -> {
                        spanningGraph.forEachRelationship(nodeId, 1.0, (s, t, w) ->
                            {
                                relationshipsBuilder.addFromInternal(s, t, w);
                                return true;
                            }
                        );
                        return true;
                    }
                );

            }
            relationships=relationshipsBuilder.build();
            computationResult
                .graphStore()
                .addRelationshipType(
                    mutateRelationshipType,
                    Optional.of(config.weightMutateProperty()),
                    Optional.of(NumberType.FLOATING_POINT),
                    relationships
                );
            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            builder.withRelationshipsWritten(spanningTree.effectiveNodeCount() - 1);
            builder.withConfig(config);
            return Stream.of(builder.build());
        };
    }
}
