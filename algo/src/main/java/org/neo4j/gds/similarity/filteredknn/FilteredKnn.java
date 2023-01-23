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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.SimilarityFunction;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Filtered KNN is the same as ordinary KNN, _but_ we allow users to regulate final output in two ways.
 *
 * Consider each result item to be a relationship from one node to another, with a score.
 *
 * Firstly, we enable source node filtering. This limits the result to only contain relationships where the source node matches the filter.
 * Secondly, we enable target node filtering. This limits the result to only contain relationships where the target node matches the filter.
 *
 * In both cases the source or target node set can be actual specified nodes, or it could be all nodes with a label.
 */
@SuppressWarnings("ClassWithOnlyPrivateConstructors")
public class FilteredKnn extends Algorithm<FilteredKnnResult> {
    /**
     * This is KNN instrumented with neighbour consumers and similarity function
     */
    private final Knn delegate;

    private final TargetNodeFiltering targetNodeFiltering;
    private final NodeFilter sourceNodeFilter;

    public static FilteredKnn createWithoutSeeding(Graph graph, FilteredKnnBaseConfig config, KnnContext context) {
        return create(graph, config, context, Optional.empty());
    }

    // a bit speculative, but we imagine this being used as entrypoint for seeding
    public static FilteredKnn createWithDefaultSeeding(Graph graph, FilteredKnnBaseConfig config, KnnContext context) {
        var similarityFunction = Knn.defaultSimilarityFunction(graph, config.nodeProperties());

        return create(graph, config, context, Optional.of(similarityFunction));
    }

    /**
     * Subtle here, but if you do not provide a similarity function, we won't seed; however, we will immediately create
     * a default similarity function to use for KNN. Meaning, passing in the default similarity function is not the same
     * as leaving it empty, only nearly
     *
     * @param optionalSimilarityFunction An actual similarity function if you want seeding, empty otherwise
     */
    static FilteredKnn create(Graph graph, FilteredKnnBaseConfig config, KnnContext context, Optional<SimilarityFunction> optionalSimilarityFunction) {
        var targetNodeFilter = config.targetNodeFilter().toNodeFilter(graph);
        var targetNodeFiltering = TargetNodeFiltering.create(graph.nodeCount(),config.boundedK(graph.nodeCount()), targetNodeFilter, graph, optionalSimilarityFunction, config.similarityCutoff());
        var similarityFunction = optionalSimilarityFunction.orElse(Knn.defaultSimilarityFunction(graph, config.nodeProperties()));
        var knn = Knn.createWithDefaultsAndInstrumentation(graph, config, context, targetNodeFiltering, similarityFunction);
        var sourceNodeFilter = config.sourceNodeFilter().toNodeFilter(graph);

        return new FilteredKnn(context.progressTracker(), knn, targetNodeFiltering, sourceNodeFilter);
    }

    private FilteredKnn(
        ProgressTracker progressTracker,
        Knn delegate,
        TargetNodeFiltering targetNodeFiltering,
        NodeFilter sourceNodeFilter
    ) {
        super(progressTracker);
        this.delegate = delegate;
        this.targetNodeFiltering = targetNodeFiltering;
        this.sourceNodeFilter = sourceNodeFilter;
    }

    @Override
    public FilteredKnnResult compute() {
        Knn.Result result = delegate.compute();

        return ImmutableFilteredKnnResult.of(
            result.ranIterations(),
            result.didConverge(),
            result.nodePairsConsidered(),
            targetNodeFiltering,
            sourceNodeFilter
        );
    }

    long nodeCount() {
        return delegate.nodeCount();
    }

    ExecutorService executorService() {
        return delegate.executorService();
    }
}
