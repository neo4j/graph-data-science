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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Similarity mutations are all the same
 */
class SimilarityMutation {

    private final MutateRelationshipService mutateRelationshipService;
    private final TerminationFlag terminationFlag;

    SimilarityMutation(MutateRelationshipService mutateRelationshipService,TerminationFlag terminationFlag) {
        this.mutateRelationshipService = mutateRelationshipService;
        this.terminationFlag = terminationFlag;
    }

    Pair<RelationshipsWritten, Map<String, Object>> execute(
        Graph graph,
        GraphStore graphStore,
        String mutateRelationshipType,
        String mutateRelationshipProperty,
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream,
        boolean shouldComputeSimilarityDistribution
    ) {
        var similarityGraph =  new SimilarityGraphBuilder(
            graph,
            concurrency,
            DefaultPool.INSTANCE,
            terminationFlag,
            shouldComputeSimilarityDistribution
        ).build(similarityResultStream);

        return execute(
                graphStore,
                mutateRelationshipType, mutateRelationshipProperty,
                similarityGraph
        );
    }

    Pair<RelationshipsWritten, Map<String, Object>> execute(
        GraphStore graphStore,
        String mutateRelationshipType,
        String mutateRelationshipProperty,
        SimilarityGraph similarityGraph
    ) {

        var relationships = similarityGraph.relationships(mutateRelationshipType,mutateRelationshipProperty);

        var relationshipsWritten = mutateRelationshipService.mutate(
            graphStore,
            relationships
        );

        var similaritySummary = similarityGraph.similarityDistribution();

        return Pair.of(relationshipsWritten, similaritySummary);
    }
}
