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
package org.neo4j.gds.mcmf;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.maxflow.Relationships;

public class CostRelationships implements Relationships {

    private final Relationships defaultRelationships;
    private final HugeDoubleArray cost;
    private final long originalGraphRelationshipCount;
    private long maxIndex = -1;

    CostRelationships(
        Relationships defaultRelationships,
        HugeDoubleArray cost,
        long originalGraphRelationshipCount
    ) {
        this.defaultRelationships = defaultRelationships;
        this.cost = cost;
        this.originalGraphRelationshipCount = originalGraphRelationshipCount;
    }

    @Override
    public double flow(long index) {
        return  defaultRelationships.flow(index);
    }

    @Override
    public double originalCapacity(long index) {
        return defaultRelationships.originalCapacity(index);
    }

    @Override
    public void push(long index, double delta) {
         defaultRelationships.push(index,delta);
    }

    double cost(long index){
        return  index < originalGraphRelationshipCount ? cost.get(index) : 0;
    }

    double maximalUnitCost() {
        if (maxIndex!=-1) return cost.get(maxIndex);
        var max = Double.NEGATIVE_INFINITY; //fixme
        for (long r = 0; r < cost.size(); r++) {
            double rCost = cost.get(r);
            if (max < rCost){
                maxIndex=r;
                max = rCost;
            }
        }
        return max;
    }
}
