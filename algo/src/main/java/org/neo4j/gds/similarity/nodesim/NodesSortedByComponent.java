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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages nodes sorted by component. Produces an iterator over all nodes in a given component.
 */
public class NodesSortedByComponent {
    private final HugeLongArray components;
    private final HugeAtomicLongArray upperBoundPerComponent;
    private final HugeLongArray nodesSorted;

    public NodesSortedByComponent(HugeLongArray components, int concurrency) {
        this.components = components;
        this.upperBoundPerComponent = computeIndexUpperBoundPerComponent(components, concurrency);
        var componentCoordinateArray = HugeAtomicLongArray.of(components.size(), ParalleLongPageCreator.passThrough(concurrency));
        upperBoundPerComponent.copyTo(componentCoordinateArray, components.size());
        this.nodesSorted = computeNodesSortedByComponent(components, componentCoordinateArray, concurrency);
    }

    public Iterator<Long> iterator(long componentId) {
        return new Iterator<>() {
            long runningIdx = getUpperBoundPerComponent().get(componentId);
            @Override
            public boolean hasNext() {
                return getComponents().get(getNodesSorted().get(runningIdx)) == componentId;
            }

            @Override
            public Long next() {
                try {
                    return getNodesSorted().get(runningIdx--);
                } catch (ArrayIndexOutOfBoundsException ex) {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    HugeLongArray getComponents() {
        return components;
    }

    HugeAtomicLongArray getUpperBoundPerComponent() {
        return upperBoundPerComponent;
    }

    HugeLongArray getNodesSorted() {
        return nodesSorted;
    }

    static HugeAtomicLongArray computeIndexUpperBoundPerComponent(HugeLongArray components, int concurrency) {
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

    static HugeLongArray computeNodesSortedByComponent(HugeLongArray components,
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
