/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.doc;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.cc.ConnectedComponentsPregel;
import org.neo4j.graphalgo.beta.pregel.cc.ImmutableConnectedComponentsConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.neo4j.graphalgo.beta.pregel.cc.ConnectedComponentsPregel.COMPONENT;

class PregelConnectedComponentsDocExample {
    @Test
    void testDoc() {
        int batchSize = 10;
        int maxIterations = 10;

        var config = ImmutableConnectedComponentsConfig.builder()
            .maxIterations(maxIterations)
            .isAsynchronous(true)
            .build();

        var randomGraph = RandomGraphGenerator
            .builder()
            .nodeCount(100)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .allocationTracker(AllocationTracker.EMPTY)
            .build()
            .generate();

        var pregelJob = Pregel.create(
            randomGraph,
            config,
            new ConnectedComponentsPregel(),
            batchSize,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeLongArray nodeValues = pregelJob.run().nodeValues().longProperties(COMPONENT);
        System.out.println(nodeValues.toString());
    }
}
