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

import org.neo4j.gds.algorithms.similarity.specificfields.KnnSpecificFields;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Stream;

final class SimilarityResultCompanion {

    static SimilarityGraphResult computeToGraph(
        Graph graph,
        long nodeCount,
        int concurrency,
        Stream<SimilarityResult> similarityResultStream
    ) {

        Graph similarityGraph = new SimilarityGraphBuilder(
            graph,
            concurrency,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        ).build(similarityResultStream);

        return new SimilarityGraphResult(similarityGraph, nodeCount, false);
    }

    static SpecificFieldsWithSimilarityStatisticsSupplier<KnnResult, KnnSpecificFields> KNN_SPECIFIC_FIELDS_SUPPLIER = (result, similarityDistribution) -> {
        return new KnnSpecificFields(
            result.nodesCompared(),
            result.nodePairsConsidered(),
            result.didConverge(),
            result.ranIterations(),
            result.totalSimilarityPairs(),
            similarityDistribution
        );
    };

    static SpecificFieldsWithSimilarityStatisticsSupplier<FilteredKnnResult, KnnSpecificFields> FILTERED_KNN_SPECIFIC_FIELDS_SUPPLIER = (result, similarityDistribution) -> {
        return new KnnSpecificFields(
            result.nodesCompared(),
            result.nodePairsConsidered(),
            result.didConverge(),
            result.ranIterations(),
            result.numberOfSimilarityPairs(),
            similarityDistribution
        );
    };

    static SpecificFieldsWithSimilarityStatisticsSupplier<NodeSimilarityResult, SimilaritySpecificFieldsWithDistribution> NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER = ((result, similarityDistribution) -> {
        var graphResult = result.graphResult();
        return new SimilaritySpecificFieldsWithDistribution(
            graphResult.comparedNodes(),
            graphResult.similarityGraph().relationshipCount(),
            similarityDistribution
        );
    });

    private SimilarityResultCompanion() {}
}
