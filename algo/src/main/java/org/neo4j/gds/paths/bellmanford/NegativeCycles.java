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
package org.neo4j.gds.paths.bellmanford;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

import java.util.concurrent.atomic.AtomicLong;

final class NegativeCycles {

     private final HugeLongArray  negativeCycles;
    private HugeAtomicBitSet stored;
    private final boolean trackNegativeCycles;
    private final AtomicLong  negativeCycleIndex = new AtomicLong();

    private NegativeCycles(HugeLongArray negativeCycles, HugeAtomicBitSet stored, boolean trackNegativeCycles) {
        this.negativeCycles = negativeCycles;
        this.stored = stored;
        this.trackNegativeCycles = trackNegativeCycles;
    }

    static NegativeCycles create(long nodeCount, boolean trackNegativeCycles){
            if (!trackNegativeCycles){
                return new NegativeCycles(null,null,false);
            }
            var negCycles = HugeLongArray.newArray(nodeCount);
            var stored = HugeAtomicBitSet.create(nodeCount);
            return new NegativeCycles(negCycles,stored,true);

    }

    void considerAsStartNode(long node){
        if (!trackNegativeCycles){
            negativeCycleIndex.getAndIncrement();
        }else {
            if (!stored.getAndSet(node)) {
                //we do not want to store the same node more than once (because it might lead to duplication/or AIOOB)
                negativeCycles.set(negativeCycleIndex.getAndIncrement(), node);
            }
        }
    }
    boolean trackNegativeCycles(){
        return  trackNegativeCycles;
    }

    long numberOfNegativeCycles(){
        return negativeCycleIndex.get();
    }

    boolean containsNegativeCycles(){
        return numberOfNegativeCycles() > 0;
    }

    HugeLongArray negativeCycles(){
        return  negativeCycles;
    }

}
