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
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnContext;

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
public final class FilteredKnn extends Algorithm<FilteredKnnResult> {
    /**
     * This is KNN instrumented with neighbour consumers
     */
    private final Knn delegate;

    private final TargetNodeFiltering targetNodeFiltering;
    private final NodeFilter sourceNodeFilter;

    public static FilteredKnn create(Graph graph, FilteredKnnBaseConfig config, KnnContext context) {
        var targetNodeFilter = config.targetNodeFilter().toNodeFilter(graph);
        var targetNodeFiltering = TargetNodeFiltering.create(graph.nodeCount(), config.boundedK(graph.nodeCount()), targetNodeFilter);
        var knn = Knn.createWithDefaultsAndInstrumentation(graph, config, context, targetNodeFiltering);
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

    @Override
    public void release() {
        delegate.release();
    }

    @Override
    public void assertRunning() {
        delegate.assertRunning();
    }
}
