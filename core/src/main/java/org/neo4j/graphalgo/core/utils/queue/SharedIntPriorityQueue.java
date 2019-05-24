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
package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.hppc.IntDoubleMap;


/**
 * An IntPriorityQueue that holds the smallest value on top and uses a
 * shared map for the costs that are associated with the values.
 * The queue will only ever read costs and never write them. It is up to the
 * user of this queue to maintain the correct costs.
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class SharedIntPriorityQueue extends IntPriorityQueue {

    protected final IntDoubleMap costs;
    protected final double defaultCost;

    /**
     * Creates a new queue with the given capacity.
     * The queue dynamically grows to hold all elements.
     * The costs map is shared with the caller for reads and never modified
     * by the queue itself; it is up to the caller to make sure that the
     * costs are up-to-date.
     * The defaultCost is used in case a value has no entry in the costs map.
     */
    public SharedIntPriorityQueue(
            int initialCapacity,
            IntDoubleMap costs,
            double defaultCost) {
        super(initialCapacity);
        this.costs = costs;
        this.defaultCost = defaultCost;
    }

    @Override
    protected double cost(final int element) {
        return costs.getOrDefault(element, defaultCost);
    }

    @Override
    protected boolean addCost(final int element, final double cost) {
        // does nothing, costs should be managed outside of this queue
        return false;
    }

    @Override
    protected void removeCost(final int element) {
        // does nothing, costs should be managed outside of this queue
    }

    public static SharedIntPriorityQueue min(int capacity, IntDoubleMap costs, double defaultCost) {
        return new SharedIntPriorityQueue(capacity, costs, defaultCost) {
            @Override
            protected boolean lessThan(int a, int b) {
                return costs.get(a) < costs.get(b);
            }
        };
    }

    public static SharedIntPriorityQueue max(int capacity, IntDoubleMap costs, double defaultCost) {
        return new SharedIntPriorityQueue(capacity, costs, defaultCost) {
            @Override
            protected boolean lessThan(int a, int b) {
                return costs.get(a) > costs.get(b);
            }
        };
    }
}
