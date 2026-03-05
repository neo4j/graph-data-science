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
package org.neo4j.gds.similarity;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.similarity.ActualSimilaritySummaryBuilder;
import org.neo4j.gds.algorithms.similarity.SimilaritySummaryBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.similarity.nodesim.TopKGraph;

import java.util.Map;

public class TopKSimilarityGraph extends SimilarityGraph{

    private Map<String,Object> similarityDistribution = Map.of();
    private final boolean shouldComputeStatistics;

    public TopKSimilarityGraph(Graph graph, boolean shouldComputeStatistics) {
        super(graph);
        this.shouldComputeStatistics = shouldComputeStatistics;
    }

    @Override
    SingleTypeRelationships relationships(String relationshipType, String similarityPropertyName) {
        TopKGraph topKGraph = (TopKGraph) graph;

        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(topKGraph)
            .relationshipType(RelationshipType.of(relationshipType))
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(similarityPropertyName))
            .concurrency(new Concurrency(1))
            .executorService(DefaultPool.INSTANCE)
            .build();

        IdMap idMap = graph;
        boolean shouldCompute = shouldComputeDistribution();
        var similarityDistributionBuilder = SimilaritySummaryBuilder.of(new Concurrency(1),shouldCompute);
        RelationshipWithPropertyConsumer consumer = (s,t,w)->{
            relationshipsBuilder.addFromInternal(
                idMap.toRootNodeId(s),
                idMap.toRootNodeId(t),
                w
            );
            similarityDistributionBuilder.accept(s,t,w);
            return true;
        };
        traverseTop(consumer);
        var relationships = relationshipsBuilder.build();
        if (similarityDistribution.isEmpty()) {
           this.similarityDistribution = similarityDistributionBuilder.similaritySummary();
        }

       return relationships;
    }

    void traverseTop(RelationshipWithPropertyConsumer consumer){
        TopKGraph topKGraph = (TopKGraph) graph;
        topKGraph.forEachNode(nodeId -> {
            topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                consumer.accept(sourceNodeId,targetNodeId,property);
                return true;
            });
            return true;
        });
    }
    boolean shouldComputeDistribution(){
        return  shouldComputeStatistics && similarityDistribution.isEmpty();
    }
    @Override
    Map<String, Object> similarityDistribution() {

        if (shouldComputeDistribution()){
            var similaritySummaryBuilder = ActualSimilaritySummaryBuilder.create(new Concurrency(1));
            traverseTop(similaritySummaryBuilder);
            this.similarityDistribution = similaritySummaryBuilder.similaritySummary();
        }
        return similarityDistribution;
    }

    @Override
    public Graph concurrentCopy() {
        return graph.concurrentCopy();
    }
}
