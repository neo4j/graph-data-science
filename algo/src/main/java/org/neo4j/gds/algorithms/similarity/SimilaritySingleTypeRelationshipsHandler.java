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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.mutateservices.SingleTypeRelationshipsProducer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.nodesim.TopKGraph;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.result.SimilarityStatistics.computeHistogram;

public class SimilaritySingleTypeRelationshipsHandler implements SingleTypeRelationshipsProducer {

    private final boolean shouldComputeStatistics;
    private final Graph graph;
    private final Supplier<SimilarityGraphResult> similarityGraphResultSupplier;
    private Map<String,Object> similaritySummary;
    private long relationshipCount;

    public SimilaritySingleTypeRelationshipsHandler(
        Graph graph,
        Supplier<SimilarityGraphResult> similarityGraphResultSupplier,
        boolean shouldComputeStatistics
    ) {

        this.shouldComputeStatistics = shouldComputeStatistics;
        this.similarityGraphResultSupplier = similarityGraphResultSupplier;
        this.graph = graph;
    }

    public Map<String, Object> similaritySummary() {
        return similaritySummary;
    }

    @Override
    public SingleTypeRelationships createRelationships(String mutateRelationshipType, String mutateProperty) {

        RelationshipType relationshipType = RelationshipType.of(mutateRelationshipType);
        var similarityGraphResult = similarityGraphResultSupplier.get();
        SingleTypeRelationships relationships;

        if (similarityGraphResult.isTopKGraph()) {
            TopKGraph topKGraph = (TopKGraph) similarityGraphResult.similarityGraph();

            RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(topKGraph)
                .relationshipType(relationshipType)
                .orientation(Orientation.NATURAL)
                .addPropertyConfig(GraphFactory.PropertyConfig.of(mutateProperty))
                .concurrency(new Concurrency(1))
                .executorService(DefaultPool.INSTANCE)
                .build();

            IdMap idMap = graph;

            var similarityDistributionBuilder = SimilaritySummaryBuilder.of(shouldComputeStatistics);
                topKGraph.forEachNode(nodeId -> {
                        topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                            relationshipsBuilder.addFromInternal(
                                idMap.toRootNodeId(sourceNodeId),
                                idMap.toRootNodeId(targetNodeId),
                                property
                            );
                            similarityDistributionBuilder.similarityConsumer().accept(sourceNodeId,targetNodeId,property);
                            return true;
                        });
                        return true;
                    });
            relationships = relationshipsBuilder.build();
            similaritySummary = similarityDistributionBuilder.similaritySummary();
        } else {
            HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();

            relationships = SingleTypeRelationships.of(
                relationshipType,
                similarityGraph.relationshipTopology(),
                similarityGraph.schema().direction(),
                similarityGraph.relationshipProperties(),
                Optional.of(RelationshipPropertySchema.of(mutateProperty, ValueType.DOUBLE))
            );

            if (shouldComputeStatistics) {
                var histogram = computeHistogram(similarityGraph);
                similaritySummary = SimilarityStatistics.similaritySummary(histogram);
            }else{
                similaritySummary = Map.of();
            }
        }
        this.relationshipCount = similarityGraphResult.similarityGraph().relationshipCount();
        return relationships;
    }

    @Override
    public long relationshipsCount() {
        return relationshipCount;
    }
}
