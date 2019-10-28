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

package org.neo4j.graphalgo.impl.coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.impl.generator.RelationshipDistribution;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

class K1ColoringTest {

    @Test
    void test() {
        RandomGraphGenerator generator = new RandomGraphGenerator(
            1000,
            5,
            RelationshipDistribution.UNIFORM,
            Optional.empty(),
            AllocationTracker.EMPTY
        );

        HugeGraph graph = generator.generate();


        K1Coloring k1Coloring = new K1Coloring(
            graph,
            500,
            2,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        k1Coloring.compute(Direction.BOTH, 1000);

        HugeLongArray colors = k1Coloring.colors();

        MutableLong counter = new MutableLong(0);
        Set<Long> diffColors = new HashSet<>();

        graph.forEachNode((nodeId) ->{
            graph.forEachRelationship(nodeId, Direction.OUTGOING, (source, target) -> {
                if (colors.get(source) == colors.get(target) && source != target) {
                    counter.increment();
                }

                diffColors.add(colors.get(source));
                return true;
            });
            return true;
        });

        System.out.println(k1Coloring.ranIterations());
        System.out.println(counter);
        System.out.println(diffColors.size());
        System.out.println(diffColors);

    }

}



