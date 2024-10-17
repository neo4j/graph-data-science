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
package org.neo4j.gds.pricesteiner;

import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.function.LongPredicate;

public class ClusterEventsPriorityQueue {

    private final HugeLongPriorityQueue queue;

    public ClusterEventsPriorityQueue(long nodeCount) {
        this.queue = HugeLongPriorityQueue.min(nodeCount*2);
    }

    double closestEvent(LongPredicate allowed){
        var top=queue.top();
        while (!allowed.test(top)){
            queue.pop();
            top=queue.top();
        }
        return queue.cost(top);
    }

    long topCluster(){
        return queue.top();
    }

    void pop(){
            queue.pop();
    }

    void add(long cluster, double remainingMoat){
        queue.add(cluster, remainingMoat);
    }

}
