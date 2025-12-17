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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.InputNodes;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.function.LongPredicate;

public final class NodeConstraintsFromPropertyIdMap  implements NodeConstraintsIdMap{

    private final HugeLongArray toMappedNode;
    private final HugeLongArray realIndices;
    private final  long originalNodeCount;
    private final long firstMappedId;
    private final long firstRelationshipId;
    private final NodePropertyValues nodePropertyValues;

    private NodeConstraintsFromPropertyIdMap(
        HugeLongArray toMappedNode,
        HugeLongArray realIndices,
        long originalNodeCount,
        long firstMappedId,
        long firstRelationshipId,
        NodePropertyValues nodePropertyValues
    ) {
        this.toMappedNode = toMappedNode;
        this.realIndices = realIndices;
        this.originalNodeCount = originalNodeCount;
        this.firstMappedId = firstMappedId;
        this.firstRelationshipId = firstRelationshipId;
        this.nodePropertyValues = nodePropertyValues;
    }

    public static NodeConstraintsIdMap create(
        IdMap idMap,
        long relationshipCount,
        NodePropertyValues nodePropertyValues,
        InputNodes sources,
        InputNodes sinks
    ){
         LongPredicate hasValue = (s) -> !Double.isNaN(nodePropertyValues.doubleValue(s));
            var numberOfConstraints=0;
            for (long nodeId=0;nodeId<idMap.nodeCount();++nodeId){
                    if(hasValue.test(nodeId)){
                        numberOfConstraints++;
                    }
            }
            for (var node : sources.inputNodes()){
                if (hasValue.test(node)) {
                    numberOfConstraints--;
                }
            }
            for (var node : sinks.inputNodes()){
                if (hasValue.test(node)) {
                    numberOfConstraints--;
                }
            }
            if (numberOfConstraints==0) return new IgnoreNodeConstraints();
            var toMappedId= HugeLongArray.newArray(idMap.nodeCount());
            for (var node : sources.inputNodes()){
                    toMappedId.set(node,-1);
            }
            for (var node : sinks.inputNodes()){
                toMappedId.set(node,-1);
            }
            var realIndices =  HugeLongArray.newArray(numberOfConstraints);
            long index=0;
            for (long nodeId=0;nodeId<idMap.nodeCount();++nodeId){
                if (toMappedId.get(nodeId)==-1) {
                    continue;
                }
                if (hasValue.test(nodeId)){
                    toMappedId.set(nodeId,index);
                    realIndices.set(index++,nodeId);
                }else{
                    toMappedId.set(nodeId,-1);
                }
            }
            return new NodeConstraintsFromPropertyIdMap(
                toMappedId,
                realIndices,
                idMap.nodeCount(),
                idMap.nodeCount()+ 2,
                relationshipCount + sources.inputNodes().size() + sinks.inputNodes().size(),
                nodePropertyValues
            );

    }
    @Override
    public boolean isFakeNode(long nodeId) {
        return  nodeId >=firstMappedId;
    }

    @Override
    public boolean hasCapacityConstraint(long nodeId) {
        return (nodeId < originalNodeCount && toMappedNode.get(nodeId)!=-1);
    }

    @Override
    public double nodeCapacity(long nodeId) {
        if (!hasCapacityConstraint(nodeId)) throw new RuntimeException();
        return nodePropertyValues.doubleValue(nodeId);
    }

    @Override
    public double relationshipCapacity(long relationshipId) {
        if (relationshipId < firstRelationshipId) throw  new RuntimeException();
        var index = relationshipId-firstRelationshipId;
        return  nodePropertyValues.doubleValue(realIndices.get(index));
    }

    @Override
    public long toFakeNodeOf(long nodeId) {
        if (!hasCapacityConstraint(nodeId)) throw new RuntimeException();
        return toMappedNode.get(nodeId) + firstMappedId;
    }

    @Override
    public long realNodeOf(long nodeId) {
        if (nodeId < firstMappedId) throw  new RuntimeException();
        var index = nodeId- firstMappedId;
        return realIndices.get(index);
    }

    @Override
    public long numberOfCapacityNodes() {
        return realIndices.size();
    }

    @Override
    public long capacityRelId(long nodeId) {
        if (!hasCapacityConstraint(nodeId)) throw new RuntimeException();
        return (firstRelationshipId + toMappedNode.get(nodeId));
    }
}
