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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

/**
 * Manages nodes sorted by component. Produces an iterator over all nodes in a given component.
 */
public final class ComponentNodes {
    private final LongUnaryOperator components;
    private final HugeAtomicLongArray upperBoundPerComponent;
    private final HugeLongArray nodesSorted;

    private ComponentNodes(LongUnaryOperator components, HugeAtomicLongArray upperBoundPerComponent,
        HugeLongArray nodesSorted) {

        this.components = components;
        this.upperBoundPerComponent = upperBoundPerComponent;
        this.nodesSorted = nodesSorted;
    }

    public static ComponentNodes create(LongUnaryOperator components, long nodeCount, Concurrency concurrency) {
        return create(components, (v) -> true, nodeCount, concurrency);
    }

    public static ComponentNodes create(
        LongUnaryOperator components,
        LongPredicate targetNodesFilter,
        long nodeCount,
        Concurrency concurrency
    ) {
        var upperBoundPerComponent = computeIndexUpperBoundPerComponent(
            components,
            nodeCount,
            targetNodesFilter,
            concurrency
        );
        var nodesSorted = computeNodesSortedByComponent(
            components,
            upperBoundPerComponent,
            targetNodesFilter,
            concurrency
        );

        return new ComponentNodes(
            components,
            upperBoundPerComponent,
            nodesSorted
        );
    }

    public PrimitiveIterator.OfLong iterator(long componentId, long offset) {
        return new Iterator(componentId, offset);
    }

    public Spliterator.OfLong spliterator(long componentId, long offset) {
        return Spliterators.spliteratorUnknownSize(
            iterator(componentId, offset),
            Spliterator.NONNULL | Spliterator.DISTINCT
        );
    }

    LongUnaryOperator getComponents() {
        return components;
    }

    HugeAtomicLongArray getUpperBoundPerComponent() {
        return upperBoundPerComponent;
    }

    HugeLongArray getNodesSorted() {
        return nodesSorted;
    }

    static HugeAtomicLongArray computeIndexUpperBoundPerComponent(
        LongUnaryOperator components,
        long nodeCount,
        LongPredicate includeNode,
        Concurrency concurrency
    ) {

        var upperBoundPerComponent = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(concurrency));

        // init coordinate array to contain the nr of nodes per component
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes: {(comp1, 3), (comp2, 20)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency.value(), TerminationFlag.RUNNING_TRUE, nodeId -> {
            {
                if (includeNode.test(nodeId)) {
                    long componentId = components.applyAsLong(nodeId);
                    upperBoundPerComponent.getAndAdd(componentId, 1);
                }
            }
        });
        AtomicLong atomicNodeSum = new AtomicLong();
        // modify coordinate array to contain the upper bound of the global index for each component
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes, comp1 randomly accessed prior to comp2:
        // {(comp1, 2), (comp2, 22)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency.value(), TerminationFlag.RUNNING_TRUE, componentId ->
        {
            if (upperBoundPerComponent.get(componentId) > 0) {
                var nodeSum = atomicNodeSum.addAndGet(upperBoundPerComponent.get(componentId));
                upperBoundPerComponent.set(componentId, nodeSum - 1);
            } else { //component is unused, initilize to -1
                upperBoundPerComponent.set(componentId, -1);
            }
        });

        return upperBoundPerComponent;
    }

    static HugeLongArray computeNodesSortedByComponent(
            LongUnaryOperator components,
            HugeAtomicLongArray idxUpperBoundPerComponent,
            LongPredicate includeNode,
            Concurrency concurrency
    ) {

        // initialized to its max possible size of 1 node <=> 1 component in a disconnected graph
        long nodeCount = idxUpperBoundPerComponent.size();
        var nodesSortedByComponent = HugeLongArray.newArray(nodeCount);
        var nodeIdxProviderArray = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(concurrency));
        idxUpperBoundPerComponent.copyTo(nodeIdxProviderArray, nodeCount);

        // fill nodesSortedByComponent with nodeId per component-sorted, unique index
        // i.e. comp1 containing 3 nodes, comp2 containing 20 nodes, named in order of processing:
        // {(0, n3), (1, n2), (2, n1), (3, n23), .., (22, n4)}
        ParallelUtil.parallelForEachNode(nodeCount, concurrency.value(), TerminationFlag.RUNNING_TRUE, indexId ->
        {
            long nodeId = nodeCount - indexId - 1;
            if (includeNode.test(nodeId)) {
                long componentId = components.applyAsLong(nodeId);
                long nodeIdx = nodeIdxProviderArray.getAndAdd(componentId, -1);
                nodesSortedByComponent.set(nodeIdx, nodeId);
            }
        });

        return nodesSortedByComponent;
    }

    private final class Iterator implements PrimitiveIterator.OfLong {
        private final long offset;
        long runningIdx;
        final long componentId;

        Iterator(long componentId, long offset) {
            this.componentId = componentId;
            this.runningIdx = getUpperBoundPerComponent().get(componentId);
            this.offset = offset;
        }

        @Override
        public boolean hasNext() {
            if (offset < 1L) {
                return runningIdx > -1 && getComponents().applyAsLong(getNodesSorted().get(runningIdx)) == componentId;
            } else {
                while (runningIdx > -1 && getComponents().applyAsLong(getNodesSorted().get(runningIdx)) == componentId) {
                    if (getNodesSorted().get(runningIdx) < offset) {
                        runningIdx--;
                    } else {
                        return true;
                    }
                }
                return false;
            }
        }
        @Override
        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return getNodesSorted().get(runningIdx--);
        }

    }
}
