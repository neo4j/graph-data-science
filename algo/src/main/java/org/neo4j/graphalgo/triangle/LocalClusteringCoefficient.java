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
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Function;

public class LocalClusteringCoefficient extends Algorithm<LocalClusteringCoefficient, LocalClusteringCoefficient.Result> {

    private final int concurrency;
    private final AllocationTracker tracker;
    private final NodeProperties triangleCountProperty;
    private final LocalClusteringCoefficientBaseConfig configuration;

    private Graph graph;

    // Results
    private HugeDoubleArray localClusteringCoefficients;
    private double averageClusteringCoefficient;

    LocalClusteringCoefficient(
        Graph graph,
        LocalClusteringCoefficientBaseConfig configuration,
        AllocationTracker tracker,
        ExecutorService executorService,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.tracker = tracker;
        this.progressLogger = progressLogger;

        this.configuration = configuration;
        this.concurrency = configuration.concurrency();
        this.triangleCountProperty =
            Optional.ofNullable(configuration.triangleCountProperty())
                .map(graph::nodeProperties)
                .orElse(null);
    }

    @Override
    public Result compute() {

        if (null == triangleCountProperty) {
            HugeAtomicLongArray triangleCounts = computeTriangleCounts();
            calculateCoefficients(triangleCounts::get);
        } else {
            calculateCoefficients((nodeId) -> (long) triangleCountProperty.nodeProperty(nodeId, 0.0));
        }

        return Result.of(
            localClusteringCoefficients,
            averageClusteringCoefficient
        );
    }

    private void calculateCoefficients(Function<Long, Long> propertyValueFunction) {
        long nodeCount = graph.nodeCount();
        localClusteringCoefficients = HugeDoubleArray.newArray(nodeCount, tracker);

        DoubleAdder localClusteringCoefficientSum = new DoubleAdder();
        ParallelUtil.parallelForEachNode(graph, concurrency, nodeId -> {
            double localClusteringCoefficient = calculateCoefficient(
                propertyValueFunction.apply(nodeId),
                graph.degree(nodeId)
            );
            localClusteringCoefficients.set(nodeId, localClusteringCoefficient);
            localClusteringCoefficientSum.add(localClusteringCoefficient);
        });

        // compute average clustering coefficient
        averageClusteringCoefficient = localClusteringCoefficientSum.doubleValue() / nodeCount;
    }

    private HugeAtomicLongArray computeTriangleCounts() {

        IntersectingTriangleCount intersectingTriangleCount = new IntersectingTriangleCountFactory<>().build(
            graph,
            LocalClusteringCoefficientFactory.createTriangleCountConfig(configuration),
            tracker,
            progressLogger.getLog()
        );

        return intersectingTriangleCount.compute().localTriangles();
    }

    private double calculateCoefficient(long triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        // local clustering coefficient C(v) = 2 * triangles(v) / (degree(v) * (degree(v) - 1))
        return ((double) (triangles << 1)) / (degree * (degree - 1));
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
