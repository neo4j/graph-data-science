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

import java.util.Arrays;
import java.util.function.ToLongBiFunction;

class RelationshipFilterer {

    private final long[] neighbors;
    private long filteringSpurNode;
    private int allNeighbors;
    private int neighborIndex;
    private  boolean trackRelationships;
    private final ToLongBiFunction<MutablePathResult, Integer> relationshipAvoidMapper;

    RelationshipFilterer(int k,boolean trackRelationships){
        this.neighbors=new long[k];
        this.trackRelationships = trackRelationships;
        if (trackRelationships) {
            // if we are in a multi-graph, we  must store the relationships ids as they are
            //since two nodes may be connected by multiple relationships and we must know which to avoid
            relationshipAvoidMapper = (path, position) -> path.relationship(position);
        }else{
            //otherwise the graph has surely no parallel edges, we do not need to explicitly store relationship ids
            //we can just store endpoints, so that we know which nodes a node should avoid
            relationshipAvoidMapper = (path, position) -> path.node(position + 1);
        }
    }
    void addBlockingNeighbor(MutablePathResult path,int indexId){
        var avoidId = relationshipAvoidMapper.applyAsLong(path, indexId);
        neighbors[allNeighbors++] = avoidId;
    }

    void setFilter(long filteringSpurNode){
        this.filteringSpurNode = filteringSpurNode;
        this.neighborIndex = 0;
        this.allNeighbors = 0;
    }
    void prepare(){
        Arrays.sort(neighbors,0,allNeighbors);
    }
     boolean validRelationship(long source, long target, long relationshipId) {
        if (source == filteringSpurNode) {

            long forbidden = trackRelationships
                ? relationshipId
                : target;

            if (neighborIndex == allNeighbors) return true;

            while (neighbors[neighborIndex] < forbidden) {
                if (++neighborIndex == allNeighbors) {
                    return true;
                }
            }

            return neighbors[neighborIndex] != forbidden;
        }

        return true;

    }


}
