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
package org.neo4j.gds.applications.algorithms.machinelearning;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictMutateConfig;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.termination.TerminationFlag;

class KgeMutateStep implements MutateStep<KGEPredictResult, RelationshipsWritten> {
    private final TerminationFlag terminationFlag;
    private final KGEPredictMutateConfig configuration;

    KgeMutateStep(TerminationFlag terminationFlag, KGEPredictMutateConfig configuration) {
        this.terminationFlag = terminationFlag;
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        KGEPredictResult result
    ) {
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var concurrency = configuration.concurrency();

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph)
            .relationshipType(mutateRelationshipType)
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.builder()
                .propertyKey(configuration.mutateProperty())
                .build())
            .concurrency(concurrency)
            .executorService(DefaultPool.INSTANCE)
            .build();

        var similarityResultStream = result.topKMap().stream();

        ParallelUtil.parallelStreamConsume(
            similarityResultStream,
            concurrency,
            terminationFlag,
            stream -> stream.forEach(
                similarityResult -> relationshipsBuilder.addFromInternal(
                    graph.toRootNodeId(similarityResult.sourceNodeId()),
                    graph.toRootNodeId(similarityResult.targetNodeId()),
                    similarityResult.property()
                )
            )
        );

        var relationships = relationshipsBuilder.build();

        graphStore.addRelationshipType(relationships);

        return new RelationshipsWritten(relationships.topology().elementCount());
    }
}
