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
package org.neo4j.gds.paths.dijkstra;

import com.carrotsearch.hppc.BitSet;

import java.util.List;

class ManyTargets implements Targets{

     private final BitSet bitSet;
     private int targetCount;
     ManyTargets(List<Long> targetNodesList){

         long maxNodeId = 0;
         for (var targetNode : targetNodesList) {
             maxNodeId = Math.max(targetNode, maxNodeId);
         }
         bitSet = new BitSet(maxNodeId + 1);
         targetNodesList.forEach(bitSet::set);
         targetCount = targetNodesList.size();
     }
     @Override
     public TraversalState apply(long nodeId) {
         if (bitSet.get(nodeId)){
             targetCount--;
            return  (targetCount==0) ?
                        TraversalState.EMIT_AND_STOP
                     :  TraversalState.EMIT_AND_CONTINUE;
         }
         return  TraversalState.CONTINUE;
     }
 }
