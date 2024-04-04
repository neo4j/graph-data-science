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
package org.neo4j.gds.wcc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Parallel Union-Find Algorithm based on the
 * "Wait-free Parallel Algorithms for the Union-Find Problem" paper.
 *
 * @see HugeAtomicDisjointSetStruct
 * @see <a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.56.8354&rep=rep1&type=pdf">the paper</a>
 * <p>
 * For undirected graphs and directed graphs with index, we use a sampling based approach, as introduced in [1].
 * <p>
 * The idea is to identify the largest component using a sampled subgraph.
 * Relationships of nodes that are already contained in the largest component are
 * not iterated. The compression step described in [1], is contained in
 * {@link DisjointSetStruct#setIdOf}.
 * <p>
 * [1] Michael Sutton, Tal Ben-Nun, and Amnon Barak. "Optimizing Parallel
 * Graph Connectivity Computation via Subgraph Sampling" Symposium on
 * Parallel and Distributed Processing, IPDPS 2018.
 */
public class Wcc extends Algorithm<DisjointSetStruct> {

    private final WccParameters parameters;
    private final ExecutorService executorService;
    private final long batchSize;

    private final Graph graph;

    public Wcc(
        Graph graph,
        ExecutorService executor,
        int minBatchSize,
        WccParameters parameters,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.parameters = parameters;

        this.executorService = executor;

        this.batchSize = ParallelUtil.adjustedBatchSize(
            graph.nodeCount(),
            parameters.concurrency(),
            minBatchSize,
            Integer.MAX_VALUE
        );

        if (ParallelUtil.threadCount(batchSize, graph.nodeCount()) > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Too many nodes (%d) to run WCC with the given concurrency (%d) and batchSize (%d)",
                graph.nodeCount(),
                parameters.concurrency(),
                batchSize
            ));
        }
    }

    @Override
    public DisjointSetStruct compute() {
        progressTracker.beginSubTask();

        long nodeCount = graph.nodeCount();
        var disjointSetStruct = parameters.seedProperty()
            .map(seedProperty -> {
                var initialComponents = CommunityCompanion.extractSeedingNodePropertyValues(graph, seedProperty);
                return new HugeAtomicDisjointSetStruct(nodeCount, initialComponents, parameters.concurrency());
            })
            .orElseGet(() -> new HugeAtomicDisjointSetStruct(nodeCount, parameters.concurrency()));

        if (graph.characteristics().isUndirected() || graph.characteristics().isInverseIndexed()) {
            new SampledStrategyBuilder()
                .graph(graph)
                .disjointSetStruct(disjointSetStruct)
                .threshold(threshold())
                .concurrency(parameters.concurrency())
                .terminationFlag(terminationFlag)
                .progressTracker(progressTracker)
                .executorService(executorService)
                .build()
                .compute();
        } else {
            new UnsampledStrategyBuilder()
                .graph(graph)
                .disjointSetStruct(disjointSetStruct)
                .threshold(threshold())
                .batchSize(batchSize)
                .terminationFlag(terminationFlag)
                .progressTracker(progressTracker)
                .executorService(executorService)
                .build()
                .compute();
        }

        progressTracker.endSubTask();

        return disjointSetStruct;
    }

    static double defaultWeight(double threshold) {
        return threshold + 1;
    }

    private Optional<Double> threshold() {
        return parameters.hasThreshold() ? Optional.of(parameters.threshold()) : Optional.empty();
    }
}
