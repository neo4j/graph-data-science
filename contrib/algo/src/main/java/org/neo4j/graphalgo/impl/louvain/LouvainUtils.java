/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;

public class LouvainUtils {

    /**
     * normalize nodeToCommunity-Array. Maps community IDs
     * in a sequential order starting at 0.
     *
     * @param communities
     * @return number of communities
     */
    static int normalize(int[] communities) {
        final IntIntMap map = new IntIntScatterMap(communities.length);
        int c = 0;
        for (int i = 0; i < communities.length; i++) {
            int mapped, community = communities[i];
            if ((mapped = map.getOrDefault(community, -1)) != -1) {
                communities[i] = mapped;
            } else {
                map.put(community, c);
                communities[i] = c++;
            }
        }
        return c;
    }
}
