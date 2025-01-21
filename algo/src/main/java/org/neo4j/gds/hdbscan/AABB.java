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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Arrays;
import java.util.Optional;

public record AABB(double[] min, double[] max, int dimension) {


    static AABB create(
        NodePropertyValues nodePropertyValues,
        HugeLongArray ids,
        long leftIndex,
        long rightIndex,
        int dimension
    ) {

        double[] min = new double[dimension];
        Arrays.fill(min, Double.MAX_VALUE);
        double[] max = new double[dimension];
        Arrays.fill(max, Double.MIN_VALUE);
        for (long i = leftIndex; i < rightIndex; i++) {
            var point = nodePropertyValues.doubleArrayValue(ids.get(i));
            for (int j = 0; j < dimension; j++) {
                min[j] = Math.min(min[j], point[j]);
                max[j] = Math.max(max[j], point[j]);
            }
        }
        return new AABB(min, max, dimension);

    }

    static AABB createInParallel(
        NodePropertyValues nodePropertyValues,
        HugeLongArray ids,
        long leftIndex,
        long rightIndex,
        int dimension,
        Concurrency concurrency
    ) {
        var size = rightIndex - leftIndex;
        if (size <= 1000) return create(nodePropertyValues, ids, leftIndex, rightIndex, dimension);

        double[] min = new double[dimension];
        double[] max = new double[dimension];
        Arrays.fill(min, Double.MAX_VALUE);
        Arrays.fill(max, Double.MIN_VALUE);


        //maybe we want to avoid create  the local min,max array for every step
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            size,
            (partition) -> new AABBCreateTask(partition, nodePropertyValues, ids, leftIndex, dimension),
            Optional.empty()
        );

        RunWithConcurrency.builder()
            .tasks(tasks)
            .concurrency(concurrency)
            .run();

        for (var task : tasks) {
            var taskMin = task.taskMin();
            var taskMax = task.taskMax();
            for (int i = 0; i < dimension; ++i) {
                min[i] = Math.min(min[i], taskMin[i]);
                max[i] = Math.max(max[i], taskMax[i]);
            }
        }
        return new AABB(min, max, dimension);

    }


    int mostSpreadDimension() {
        double bestSpread = max[0] - min[0];
        int index = 0;
        for (int i = 1; i < dimension; i++) {
            var spread = max[i] - min[i];
            if (spread > bestSpread) {
                index = i;
                bestSpread = spread;
            }
        }
        return index;
    }

    double lowerBoundFor(double[] lookupPoint) {
        assert dimension == lookupPoint.length : "Lookup point has different dimension: " + lookupPoint.length + ". The box has dimension: " + dimension;
        double distance = 0d;
        for (int i = 0; i < dimension; i++) {
            var diff = Math.max(min[i], lookupPoint[i]) - Math.min(max[i], lookupPoint[i]);
            if (diff > 0) {
                distance += diff * diff;
            }
        }

        return Math.sqrt(distance);
    }
}
