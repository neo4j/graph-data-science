/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public class PageRankFactory{

    public static PageRank eigenvectorCentralityOf(Graph graph, PageRank.Config algoConfig, LongStream sourceNodeIds) {
        PageRankVariant pageRankVariant = new EigenvectorCentralityVariant();
        return new PageRank(AllocationTracker.EMPTY, graph, algoConfig, sourceNodeIds, pageRankVariant);
    }

    public static PageRank weightedOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return weightedOf(graph, dampingFactor, sourceNodeIds, AllocationTracker.EMPTY, false);
    }

    public static PageRank weightedOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker,
            boolean cacheWeights) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);
        return new PageRank(tracker, graph, dampingFactor, sourceNodeIds, pageRankVariant);
    }

    public static PageRank articleRankOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return articleRankOf(graph, dampingFactor, sourceNodeIds, AllocationTracker.EMPTY);
    }

    public static PageRank articleRankOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();
        return new PageRank(tracker, graph, dampingFactor, sourceNodeIds, pageRankVariant);
    }

    public static PageRank of(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return of(graph, dampingFactor, sourceNodeIds, AllocationTracker.EMPTY);
    }

    public static PageRank of(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker) {
        PageRankVariant computeStepFactory = new NonWeightedPageRankVariant();
        return new PageRank(tracker, graph, dampingFactor, sourceNodeIds, computeStepFactory);
    }

    public static PageRank of(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        return of(graph, dampingFactor, sourceNodeIds, AllocationTracker.EMPTY, pool, concurrency, batchSize);
    }

    public static PageRank of(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        PageRankVariant pageRankVariant = new NonWeightedPageRankVariant();
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public static PageRank weightedOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            boolean cacheWeights) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public static PageRank articleRankOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            AllocationTracker tracker,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public static PageRank eigenvectorCentralityOf(
            Graph graph,
            LongStream sourceNodeIds,
            AllocationTracker tracker,
            ExecutorService pool,
            int concurrency,
            int batchSize
    ) {
        PageRankVariant variant = new EigenvectorCentralityVariant();

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                1.0,
                sourceNodeIds,
                variant
        );
    }
}
