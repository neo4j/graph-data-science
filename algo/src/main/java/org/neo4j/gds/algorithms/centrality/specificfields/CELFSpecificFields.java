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
package org.neo4j.gds.algorithms.centrality.specificfields;

public class CELFSpecificFields {

    private final double totalSpread;
    private final long nodeCount;
   public static CELFSpecificFields EMPTY =new CELFSpecificFields(0D,0);


    public CELFSpecificFields(double totalSpread, long nodeCount){
        this.totalSpread=totalSpread;
        this.nodeCount=nodeCount;
    }

    public long nodeCount(){
        return  nodeCount;
    }

    public double totalSpread(){
        return  totalSpread;
    }

}
