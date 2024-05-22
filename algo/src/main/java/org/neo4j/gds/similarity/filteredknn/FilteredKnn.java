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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.ImmutableKnnResult;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNeighborFilterFactory;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.NeighborList;
import org.neo4j.gds.similarity.knn.SimilarityFunction;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Filtered KNN is the same as ordinary KNN, _but_ we allow users to regulate final output in two ways.
 * Consider each result item to be a relationship from one node to another, with a score.
 * Firstly, we enable source node filtering. This limits the result to only contain relationships where the source node matches the filter.
 * Secondly, we enable target node filtering. This limits the result to only contain relationships where the target node matches the filter.
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

    public static FilteredKnn createWithoutSeeding(Graph graph, FilteredKnnBaseConfig config, KnnContext context, TerminationFlag terminationFlag) {
        return create(graph, config, context, Optional.empty(), terminationFlag);
    }

    // a bit speculative, but we imagine this being used as entrypoint for seeding
    public static FilteredKnn createWithDefaultSeeding(Graph graph, FilteredKnnBaseConfig config, KnnContext context, TerminationFlag terminationFlag) {
        var similarityFunction = new SimilarityFunction(SimilarityComputer.ofProperties(graph, config.nodeProperties()));

        return create(graph, config, context, Optional.of(similarityFunction), terminationFlag);
    }

    /**
     * Subtle here, but if you do not provide a similarity function, we won't seed; however, we will immediately create
     * a default similarity function to use for KNN. Meaning, passing in the default similarity function is not the same
     * as leaving it empty, only nearly
     *
     * @param optionalSimilarityFunction An actual similarity function if you want seeding, empty otherwise
     */
    static FilteredKnn create(
        Graph graph,
        FilteredKnnBaseConfig config,
        KnnContext context,
        Optional<SimilarityFunction> optionalSimilarityFunction,
        TerminationFlag terminationFlag
    ) {
        var targetNodeFilter = config.targetNodeFilter().toNodeFilter(graph);
        var sourceNodeFilter = config.sourceNodeFilter().toNodeFilter(graph);

        var targetNodeFiltering = TargetNodeFiltering.create(
            sourceNodeFilter,
            graph.nodeCount(),
            config.k(graph.nodeCount()).value,
            targetNodeFilter,
            optionalSimilarityFunction,
            config.similarityCutoff(),
            config.concurrency()
        );
        var similarityFunction = optionalSimilarityFunction.orElse(new SimilarityFunction(SimilarityComputer.ofProperties(
            graph,
            config.nodeProperties()
        )));

        var knn = new Knn(
            graph,
            context.progressTracker(),
            context.executor(),
            config.k(graph.nodeCount()),
            config.concurrency(),
            1_000,
            config.maxIterations(),
            config.similarityCutoff(),
            config.perturbationRate(),
            config.randomJoins(),
            config.randomSeed(),
            config.initialSampler(),
            similarityFunction,
            new KnnNeighborFilterFactory(graph.nodeCount()),
            targetNodeFiltering,
            terminationFlag
        );

        return new FilteredKnn(context.progressTracker(), knn, targetNodeFiltering, sourceNodeFilter, terminationFlag);
    }

    private FilteredKnn(
        ProgressTracker progressTracker,
        Knn delegate,
        TargetNodeFiltering targetNodeFiltering,
        NodeFilter sourceNodeFilter,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.delegate = delegate;
        this.targetNodeFiltering = targetNodeFiltering;
        this.sourceNodeFilter = sourceNodeFilter;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public FilteredKnnResult compute() {
        var seedingSummary = targetNodeFiltering.seedingSummary();
        KnnResult result = (seedingSummary.seededOptimally()) ?
            ImmutableKnnResult.of(
                HugeObjectArray.newArray(NeighborList.class, 0),
                0,
                true,
                seedingSummary.nodePairsCompared(),
                seedingSummary.nodesCompared()
            ) : delegate.compute();

        return new FilteredKnnResult(
            targetNodeFiltering,
            result,
            sourceNodeFilter
        );
    }

    ExecutorService executorService() {
        return delegate.executorService();
    }
}
