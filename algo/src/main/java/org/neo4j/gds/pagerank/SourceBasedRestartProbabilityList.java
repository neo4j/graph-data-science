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
package org.neo4j.gds.pagerank;

import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.LongSet;

import java.util.Collection;


public class SourceBasedRestartProbabilityList implements InitialProbabilityProvider {
    private final double alpha;
    private final LongSet sourceNodes;


    SourceBasedRestartProbabilityList(double alpha, Collection<Long> sourceNodes) {
        this.alpha = alpha;
        this.sourceNodes =  new LongScatterSet(sourceNodes.size());
        sourceNodes.forEach(this.sourceNodes::add);
    }

    @Override
    public double provideInitialValue(long nodeId) {
        if (sourceNodes.contains(nodeId)) {
            return alpha;
        }
        return 0;
    }

}
