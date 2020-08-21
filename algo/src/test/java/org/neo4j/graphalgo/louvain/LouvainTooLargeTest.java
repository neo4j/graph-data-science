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
package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;

public class LouvainTooLargeTest {

    @Test
    void failsWhenAggregatingTooLargeCommunities() {
        RandomGraphGenerator randomGraphGenerator = new RandomGraphGenerator(
            100,
            100_000_000,
            RelationshipDistribution.POWER_LAW,
            -1L,
            Optional.empty(),
            Optional.empty(),
            Aggregation.NONE,
            Orientation.NATURAL,
            RandomGraphGeneratorConfig.AllowSelfLoops.NO,
            AllocationTracker.EMPTY
        );
        HugeGraph generate = randomGraphGenerator.generate();
        Louvain louvain = new Louvain(
            generate,
            LouvainStatsConfig.of("", Optional.empty(), Optional.empty(), CypherMapWrapper.empty()),
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.EMPTY
        );

        try {
            louvain.compute();
        } catch (Throwable e) {

        }
        fail("didn't throw");
    }
}
