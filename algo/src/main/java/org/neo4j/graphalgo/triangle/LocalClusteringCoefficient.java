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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class LocalClusteringCoefficient extends Algorithm<LocalClusteringCoefficient, LocalClusteringCoefficient.Result> {

    private final ExecutorService executorService;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final NodeProperties seedProperty;

    private Graph graph;

    // The result of computing triangles when no seed property is supplied
    private HugeAtomicLongArray triangleCounts;

    // Results
    private HugeDoubleArray localClusteringCoefficients;
    private double averageClusteringCoefficient;

    LocalClusteringCoefficient(
        Graph graph,
        @Nullable NodeProperties seedProperty,
        AllocationTracker tracker,
        ExecutorService executorService,
        int concurrency,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.seedProperty = seedProperty;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
        this.executorService = executorService;
        this.concurrency = concurrency;
    }

    @Override
    public Result compute() {

        if (null == seedProperty) {
            computeTriangleCounts();
            calculateCoefficients(triangleCounts::get);
        } else {
            calculateCoefficients((nodeId) -> (long) seedProperty.nodeProperty(nodeId, -1));
        }

        return Result.of(
            localClusteringCoefficients,
            averageClusteringCoefficient
        );
    }

    private void calculateCoefficients(Function<Long, Long> propertyValueFunction) {
        long nodeCount = graph.nodeCount();
        localClusteringCoefficients = HugeDoubleArray.newArray(nodeCount, tracker);
        double localClusteringCoefficientSum = 0.0;
        for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
            double localClusteringCoefficient = calculateCoefficient(
                propertyValueFunction.apply(nodeId),
                graph.degree(nodeId)
            );
            localClusteringCoefficients.set(nodeId, localClusteringCoefficient);
            localClusteringCoefficientSum += localClusteringCoefficient;
        }
        // compute average clustering coefficient
        averageClusteringCoefficient = localClusteringCoefficientSum / nodeCount;
    }

    private double calculateCoefficient(long triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        // local clustering coefficient C(v) = 2 * triangles(v) / (degree(v) * (degree(v) - 1))
        return ((double) (triangles << 1)) / (degree * (degree - 1));
    }

    private void computeTriangleCounts() {
        IntersectingTriangleCount intersectingTriangleCount = new IntersectingTriangleCount(
            graph,
            executorService,
            concurrency,
            tracker,
            progressLogger
        );

        this.triangleCounts = intersectingTriangleCount.compute().localTriangles();
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
