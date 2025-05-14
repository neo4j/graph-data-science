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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.filteredknn.FilteredKNNFactory;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnParameters;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityParameters;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnNeighborFilterFactory;
import org.neo4j.gds.similarity.knn.KnnParameters;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.SimilarityFunction;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityParameters;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.WccStub;

import java.util.Optional;

public class SimilarityAlgorithms {

    private final TerminationFlag terminationFlag;

    public SimilarityAlgorithms(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    public FilteredKnnResult filteredKnn(
        Graph graph,
        FilteredKnnParameters parameters,
        ProgressTracker progressTracker
    ) {
        var knnContext = ImmutableKnnContext
            .builder()
            .progressTracker(progressTracker)
            .executor(DefaultPool.INSTANCE)
            .build();

        var algorithm = FilteredKNNFactory.create(graph, parameters, knnContext,terminationFlag);

        return algorithm.compute();
    }

    public NodeSimilarityResult filteredNodeSimilarity(
        Graph graph,
        FilteredNodeSimilarityParameters  params,
        ProgressTracker progressTracker
    ) {
        var sourceNodeFilter = params.filteringParameters().sourceFilter().toNodeFilter(graph);
        var targetNodeFilter = params.filteringParameters().targetFilter().toNodeFilter(graph);

        var wccStub = new WccStub(terminationFlag);

        var algorithm = new NodeSimilarity(
            graph,
            params.nodeSimilarityParameters(),
            DefaultPool.INSTANCE,
            progressTracker,
            sourceNodeFilter,
            targetNodeFilter,
            terminationFlag,
            wccStub
        );

        return algorithm.compute();
    }

    KnnResult knn(Graph graph, KnnParameters parameters, ProgressTracker progressTracker) {

        var algorithm = Knn.create(
            graph,
            parameters,
            new SimilarityFunction(SimilarityComputer.ofProperties(graph, parameters.nodePropertySpecs())),
            new KnnNeighborFilterFactory(graph.nodeCount()),
            Optional.empty(),
            ImmutableKnnContext
                .builder()
                .progressTracker(progressTracker)
                .executor(DefaultPool.INSTANCE)
                .build(),
            terminationFlag
        );

        return algorithm.compute();
    }



    public NodeSimilarityResult nodeSimilarity(
        Graph graph,
        NodeSimilarityParameters  parameters,
        ProgressTracker progressTracker
    ) {
        var wccStub = new WccStub(terminationFlag);

        var algorithm = new NodeSimilarity(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            terminationFlag,
            wccStub
        );

        return algorithm.compute();
    }


}
