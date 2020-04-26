/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.concurrent.ExecutorService;

public class LocalClusteringCoefficient extends Algorithm<LocalClusteringCoefficient, LocalClusteringCoefficient.Result> {

    private final ExecutorService executorService;
    private final int concurrency;
    private final AllocationTracker tracker;

    private Graph graph;

    // Results
    private HugeDoubleArray localClusteringCoefficients;
    private double averageClusteringCoefficient;

    public LocalClusteringCoefficient(
        Graph graph,
        AllocationTracker tracker,
        ExecutorService executorService,
        int concurrency,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
        this.executorService = executorService;
        this.concurrency = concurrency;
    }

    @Override
    public Result compute() {

        // TODO: Make this conditional on whether a `seedProperty` is specified or not
        computeTriangleCounts();

        return Result.of(
            localClusteringCoefficients,
            averageClusteringCoefficient
        );
    }

    private void computeTriangleCounts() {
        IntersectingTriangleCount intersectingTriangleCount = new IntersectingTriangleCount(
            graph,
            executorService,
            concurrency,
            tracker,
            progressLogger
        );
        IntersectingTriangleCount.TriangleCountResult compute = intersectingTriangleCount.compute();
        localClusteringCoefficients = compute.localClusteringCoefficients();
        averageClusteringCoefficient = compute.averageClusteringCoefficient();
    }

    @Override
    public LocalClusteringCoefficient me() {
        return this;
    }

    @Override
    public void release() {
        localClusteringCoefficients = null;
        graph = null;
    }

    @ValueClass
    interface Result {

        HugeDoubleArray localClusteringCoefficients();

        double averageClusteringCoefficient();

        static Result of(
            HugeDoubleArray localClusteringCoefficients,
            double averageClusteringCoefficient
        ) {
            return ImmutableResult
                .builder()
                .localClusteringCoefficients(localClusteringCoefficients)
                .averageClusteringCoefficient(averageClusteringCoefficient)
                .build();
        }
    }
}
