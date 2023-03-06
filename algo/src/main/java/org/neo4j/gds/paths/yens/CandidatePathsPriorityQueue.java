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
package org.neo4j.gds.paths.yens;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

 class CandidatePathsPriorityQueue {

    private final ReentrantLock candidateLock;
    private final PriorityQueue<MutablePathResult> candidates;

    CandidatePathsPriorityQueue(){
        this.candidateLock=new ReentrantLock();
        this.candidates=initCandidatesQueue();
     }

    void addPath(MutablePathResult rootPath){
        candidateLock.lock();
        if (!candidates.contains(rootPath)) {
            candidates.add(rootPath);
        }
        candidateLock.unlock();
    }

    MutablePathResult pop(){
        return candidates.poll();
    }

    boolean isEmpty(){
        return candidates.isEmpty();
    }

     @NotNull
     private PriorityQueue<MutablePathResult> initCandidatesQueue() {
         return new PriorityQueue<>(Comparator
             .comparingDouble(MutablePathResult::totalCost)
             .thenComparingInt(MutablePathResult::nodeCount));
     }

 }
