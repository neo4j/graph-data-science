/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.unionfind;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.stream.Stream;

abstract class UnionFindTestBase {

    static GraphDatabaseAPI DB;
    static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    static Stream<Arguments> parameters() {
        return TestSupport.allTypesWithoutCypher().
                flatMap(graphType -> Arrays.stream(UnionFindType.values())
                        .map(ufType -> Arguments.of(graphType, ufType)));
    }

    abstract int communitySize();

    DisjointSetStruct run(UnionFindType uf, Graph graph, UnionFind.Config config) {
        return UnionFindHelper.run(
                uf,
                graph,
                Pools.DEFAULT,
                communitySize() / Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT_CONCURRENCY,
                config,
                AllocationTracker.EMPTY);
    }

    /**
     * Compute number of sets present.
     */
    static long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

}
