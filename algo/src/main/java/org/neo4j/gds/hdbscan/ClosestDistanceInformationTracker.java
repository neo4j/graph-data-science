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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

class ClosestDistanceInformationTracker {

    private final HugeDoubleArray componentClosestDistance;
    private final HugeLongArray componentInsideBestNode;
    private final HugeLongArray componentOutsideBestNode;

     ClosestDistanceInformationTracker(
        HugeDoubleArray componentClosestDistance,
         HugeLongArray componentInsideBestNode,
         HugeLongArray componentOutsideBestNode
    ) {
        this.componentClosestDistance = componentClosestDistance;
        this.componentInsideBestNode = componentInsideBestNode;
        this.componentOutsideBestNode = componentOutsideBestNode;
        reset(componentClosestDistance.size());
    }

    static ClosestDistanceInformationTracker create(long size){

        var  componentClosestDistance = HugeDoubleArray.newArray(size);
        var  componentInsideBestNode = HugeLongArray.newArray(size);
        var  componentOutsideBestNode = HugeLongArray.newArray(size);

        return new ClosestDistanceInformationTracker(componentClosestDistance,componentInsideBestNode,componentOutsideBestNode);
    }

    void reset(long upTo){
         for (long u=0;u<upTo;++u){
             componentClosestDistance.set(u,Double.MAX_VALUE);
             componentInsideBestNode.set(u,-1);
             componentOutsideBestNode.set(u,-1);

         }
    }

    void  consider(long comp1, long comp2,  long p1, long p2 , double distance){
            tryToAssign(comp1, p1, p2, distance);
            tryToAssign(comp2, p2, p1, distance);
    }

     boolean tryToAssign(long comp, long pInside,long pOutside, double distance){
        var best = componentClosestDistance.get(comp);
        if (best > distance){
            componentClosestDistance.set(comp,distance);
            componentInsideBestNode.set(comp,pInside);
            componentOutsideBestNode.set(comp,pOutside);
            return true;
        }
        return false;

    }

    double componentClosestDistance(long componentId){
         return  componentClosestDistance.get(componentId);
    }

    long componentInsideBestNode(long componentId){
         return componentInsideBestNode.get(componentId);
    }

    long componentOutsideBestNode(long componentId){
        return componentOutsideBestNode.get(componentId);
    }

}
