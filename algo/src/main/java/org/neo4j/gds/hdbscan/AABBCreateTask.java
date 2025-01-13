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
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.Arrays;

public class AABBCreateTask  implements  Runnable{

    private final double[] localMin;
    private final double[] localMax;
    private final Partition partition;
    private final NodePropertyValues nodePropertyValues;
    private final HugeLongArray ids;
    private final int dimension;
    private final long offset;

    AABBCreateTask(
        Partition partition,
        NodePropertyValues nodePropertyValues,
        HugeLongArray ids,
        long offset,
        int dimension
    ) {
        this.localMin = new double[dimension];
        Arrays.fill(localMin, Double.MAX_VALUE);
        this.localMax = new double[dimension];
        Arrays.fill(localMax, Double.MIN_VALUE);
        this.partition = partition;
        this.nodePropertyValues = nodePropertyValues;
        this.ids = ids;
        this.dimension = dimension;
        this.offset = offset;
    }

    @Override
    public void run() {
        var start = offset+ partition.startNode();
        var end  =  start + partition.nodeCount();
        for (long i = start; i < end; i++) {
            var point = nodePropertyValues.doubleArrayValue(ids.get(i));
            for (int j = 0; j < dimension; j++) {
                localMin[j] = Math.min(localMin[j], point[j]);
                localMax[j] = Math.max(localMax[j], point[j]);
            }
        }
    }

    double[]  taskMin(){
        return  localMin;
    }

    double[]  taskMax(){
        return  localMax;
    }

}
