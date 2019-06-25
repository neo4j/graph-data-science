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
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public class PageRankFactory{

    public static PageRank eigenvectorCentralityOf(Graph graph, Direction direction, LongStream sourceNodeIds) {
        PageRankVariant pageRankVariant = new EigenvectorCentralityVariant();

        return new PageRank(AllocationTracker.EMPTY, graph, direction, 1.0, sourceNodeIds, pageRankVariant);
    }

    public static PageRank weightedOf(
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds
    ) {
        return weightedOf(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph, direction, false);
    }

    public static PageRank weightedOf(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph,
            Direction direction,
            boolean cacheWeights) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);

        return new PageRank(tracker, graph, direction, dampingFactor, sourceNodeIds, pageRankVariant);
    }

    public static PageRank articleRankOf(
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return articleRankOf(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph, direction);
    }

    public static PageRank articleRankOf(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph,
            Direction direction) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();

        return new PageRank(tracker, graph, direction, dampingFactor, sourceNodeIds, pageRankVariant);
    }

    public static PageRank of(
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return of(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph, direction);
    }

    public static PageRank of(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph,
            Direction direction) {
        PageRankVariant computeStepFactory = new NonWeightedPageRankVariant();

        return new PageRank(tracker, graph, direction, dampingFactor, sourceNodeIds, computeStepFactory);
    }

    public static PageRank of(
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        return of(AllocationTracker.EMPTY, graph, direction, dampingFactor, sourceNodeIds, pool, concurrency, batchSize);
    }

    public static PageRank of(
            AllocationTracker tracker,
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds,
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
                direction, dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public static PageRank weightedOf(
            AllocationTracker tracker,
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds,
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
                direction, dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public static PageRank articleRankOf(
            AllocationTracker tracker,
            Graph graph,
            Direction direction,
            double dampingFactor,
            LongStream sourceNodeIds,
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
                direction, dampingFactor,
                sourceNodeIds,
                pageRankVariant
        );

    }

    public static PageRank eigenvectorCentralityOf(
            AllocationTracker tracker,
            Graph graph,
            Direction direction,
            LongStream sourceNodeIds,
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
                direction, 1.0,
                sourceNodeIds,
                variant
        );
    }
}
