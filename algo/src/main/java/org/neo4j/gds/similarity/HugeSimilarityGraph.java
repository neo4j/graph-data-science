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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.util.Map;
import java.util.Optional;

public class HugeSimilarityGraph extends SimilarityGraph{

    private  final Map<String,Object> similarityDistribution;

    public HugeSimilarityGraph(Graph graph, Map<String, Object> similarityDistribution) {
        super(graph);
        this.similarityDistribution = similarityDistribution;
    }

    @Override
    SingleTypeRelationships relationships(String relationshipType, String similarityPropertyName) {
        HugeGraph similarityGraph = (HugeGraph) graph;

        return SingleTypeRelationships.of(
            RelationshipType.of(relationshipType),
            similarityGraph.relationshipTopology(),
            similarityGraph.schema().direction(),
            similarityGraph.relationshipProperties(),
            Optional.of(RelationshipPropertySchema.of(similarityPropertyName, ValueType.DOUBLE))
        );
    }

    @Override
    Map<String, Object> similarityDistribution() {
        return similarityDistribution;
    }

    @Override
    public Graph concurrentCopy() {
        return graph.concurrentCopy();
    }
}
