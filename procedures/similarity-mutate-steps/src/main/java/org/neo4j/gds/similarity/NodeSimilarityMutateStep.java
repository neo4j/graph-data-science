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
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;

public final class NodeSimilarityMutateStep implements MutateStep<NodeSimilarityResult, Pair<RelationshipsWritten, Map<String, Object>>> {
    private final SimilarityMutation similarityMutation;
    private final String mutateRelationshipType;
    private final String mutateProperty;
    private final TerminationFlag terminationFlag;
    private final Concurrency concurrency;
    private final boolean shouldComputeSimilarityDistribution;

    private NodeSimilarityMutateStep(
        SimilarityMutation similarityMutation,
        String mutateRelationshipType,
        String mutateProperty,
        boolean shouldComputeSimilarityDistribution,
        Concurrency concurrency,
        TerminationFlag terminationFlag
    ) {
        this.similarityMutation = similarityMutation;
        this.mutateRelationshipType = mutateRelationshipType;
        this.mutateProperty = mutateProperty;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    public static NodeSimilarityMutateStep create(
        MutateRelationshipService mutateRelationshipService,
        String mutateRelationshipType,
        String mutateProperty,
        boolean shouldComputeSimilarityDistribution,
        Concurrency concurrency,
        TerminationFlag terminationFlag
    ) {
        var similarityMutation = new SimilarityMutation(
            mutateRelationshipService,
            terminationFlag
        );

        return new NodeSimilarityMutateStep(
            similarityMutation,
            mutateRelationshipType,
            mutateProperty,
            shouldComputeSimilarityDistribution,
            concurrency,
            terminationFlag
        );
    }

    @Override
    public Pair<RelationshipsWritten, Map<String, Object>> execute(
        Graph graph,
        GraphStore graphStore,
        NodeSimilarityResult result
    ) {

        var graphResult = new SimilarityGraphBuilder(
            graph,
            concurrency,
            DefaultPool.INSTANCE,
            terminationFlag,
            shouldComputeSimilarityDistribution
        ).build(result).graph();

        return similarityMutation.execute(
            graphStore,
            mutateRelationshipType,
            mutateProperty,
            graphResult
        );
    }
}
