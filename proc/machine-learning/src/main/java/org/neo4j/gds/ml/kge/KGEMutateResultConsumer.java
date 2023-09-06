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

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.ResultBuilderFunction;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.similarity.nodesim.TopKMap;

import java.util.stream.Stream;

class KGEMutateResultConsumer extends MutateComputationResultConsumer<TopKMapComputer, KGEPredictResult, KGEPredictMutateConfig, KGEMutateResult> {

    KGEMutateResultConsumer(ResultBuilderFunction<TopKMapComputer, KGEPredictResult, KGEPredictMutateConfig, KGEMutateResult> resultBuilderFunction) {
        super(resultBuilderFunction);
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<TopKMapComputer, KGEPredictResult, KGEPredictMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var graphStore = computationResult.graphStore();
        var graph = graphStore.getGraph();

        var config = computationResult.config();
        var concurrency = config.concurrency();
        var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph)
            .relationshipType(mutateRelationshipType)
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(config.mutateRelationshipProperty()))
            .concurrency(concurrency)
            .executorService(Pools.DEFAULT)
            .build();

        var resultWithHistogramBuilder = (KGEMutateResult.Builder) resultBuilder;
        var similarityResultStream = computationResult.result()
            .map(KGEPredictResult::topKMap)
            .map(TopKMap::stream)
            .orElseGet(Stream::empty);

        ParallelUtil.parallelStreamConsume(
            similarityResultStream,
            concurrency,
            TerminationFlag.wrap(executionContext.terminationMonitor()),
            stream -> stream.forEach(similarityResult -> {
                relationshipsBuilder.addFromInternal(
                    graph.toRootNodeId(similarityResult.sourceNodeId()),
                    graph.toRootNodeId(similarityResult.targetNodeId()),
                    similarityResult.property()
                );
            }));

        var relationships = relationshipsBuilder.build();
        computationResult
            .graphStore()
            .addRelationshipType(relationships);

        resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());

    }
}
