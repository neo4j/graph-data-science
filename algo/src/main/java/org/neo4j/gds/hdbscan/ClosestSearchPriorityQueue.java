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

import java.util.Comparator;
import java.util.PriorityQueue;

 class ClosestSearchPriorityQueue {

    private final PriorityQueue<Neighbour> priorityQueue;
    private final int numberOfClosestNeighbors;

     ClosestSearchPriorityQueue(int numberOfClosestNeighbors){
        this.numberOfClosestNeighbors = numberOfClosestNeighbors;
        priorityQueue = new PriorityQueue<>(
            numberOfClosestNeighbors,
            Comparator.comparingDouble(Neighbour::distance).reversed()
        );
    }

    public Neighbour[] closest() {
        return priorityQueue.toArray(new Neighbour[0]);
    }

    public void offer(Neighbour candidate) {
        if (priorityQueue.size() < numberOfClosestNeighbors) {
            priorityQueue.offer(candidate);
        } else {
            var top = priorityQueue.peek();
            assert top != null;
            if (top.distance() > candidate.distance()) {
                priorityQueue.poll();
                priorityQueue.offer(candidate);
            }
        }
    }

    public long size() {
        return priorityQueue.size();
    }

     public boolean largerThanLowerBound(double low) {
         assert !priorityQueue.isEmpty();
         return priorityQueue.peek().distance() > low;
     }
 }
