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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Iterator over all nodes in a given component. Contains utility functions to determine a unique index per
 * node and the index bounds per component.
 */
public class ComponentIterator implements Iterator<Long> {
    private long runningIdx;
    private long componentId;
    private HugeLongArray components;
    private HugeLongArray nodesSortedByComponent;

    public ComponentIterator(long componentId, HugeLongArray components, HugeLongArray nodesSortedByComponent,
        HugeAtomicLongArray upperBoundPerComponent) {

        this.componentId = componentId;
        this.components = components;
        this.nodesSortedByComponent = nodesSortedByComponent;
        this.runningIdx = upperBoundPerComponent.get(componentId);
    }

    @Override
    public boolean hasNext() {
        return components.get(nodesSortedByComponent.get(runningIdx)) == componentId;
    }

    @Override
    public Long next() {
        return nodesSortedByComponent.get(runningIdx--);
    }

    public static HugeAtomicLongArray getIndexUpperBoundPerComponent(HugeLongArray components, int concurrency) {
        long nodeCount = components.size();
        var upperBoundPerComponent = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(concurrency));

        // init coordinate array to contain the nr of nodes per component
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes: {(comp1, 3), (comp2, 20)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, nodeId -> {
            {
                long componentId = components.get(nodeId);
                upperBoundPerComponent.getAndAdd(componentId, 1);
            }
        });
        AtomicLong atomicNodeSum = new AtomicLong();
        // modify coordinate array to contain the end of a component's range in the order called
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes: {(comp1, 3), (comp2, 23)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, componentId ->
        {
            if (upperBoundPerComponent.get(componentId) > 0) {
                var upperIndex = atomicNodeSum.addAndGet(upperBoundPerComponent.get(componentId));
                upperBoundPerComponent.set(componentId, upperIndex);
            }
        });

        return upperBoundPerComponent;
    }

    public static HugeLongArray getNodesSortedByComponent(HugeLongArray components,
        HugeAtomicLongArray componentCoordinateArray, int concurrency) {

        long nodeCount = components.size();
        var nodesSortedByComponent = HugeLongArray.newArray(nodeCount);

        // fill nodesSortedByComponent with nodeId per unique index
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes, named in their order of processing:
        // {(0, n3), (1, n2), (2, n1), (3, n23), .., (22, n4)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, indexId ->
        {
            long nodeId = nodeCount - indexId - 1;
            long componentId = components.get(nodeId);
            long coordinate = componentCoordinateArray.getAndAdd(componentId, -1);
            nodesSortedByComponent.set(coordinate - 1, nodeId);
        });

        return nodesSortedByComponent;
    }
}
