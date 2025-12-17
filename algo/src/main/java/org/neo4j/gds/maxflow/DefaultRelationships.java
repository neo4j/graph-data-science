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

import org.neo4j.gds.collections.ha.HugeDoubleArray;

public class DefaultRelationships implements  Relationships {

    private final HugeDoubleArray originalCapacity;
    private final HugeDoubleArray flow;
    private final NodeConstraintsIdMap idMap;


    public DefaultRelationships(HugeDoubleArray originalCapacity, HugeDoubleArray flow, NodeConstraintsIdMap idMap) {
        this.originalCapacity = originalCapacity;
        this.flow = flow;
        this.idMap = idMap;
    }

   public double flow(long index){
        return flow.get(index);
    }

    public double originalCapacity(long index){
        if (index < originalCapacity.size()) return originalCapacity.get(index);
        return  idMap.relationshipCapacity(index);
    }

    public void push(long index, double delta){
        flow.addTo(index,delta);
    }
}
