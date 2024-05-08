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
package org.neo4j.gds.graphsampling.samplers.rw;

import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.Optional;

public final class RandomWalkCompanion {
    public static final double TOTAL_WEIGHT_MISSING = -1.0;

    public static Optional<HugeAtomicDoubleArray> initializeTotalWeights(RandomWalkWithRestartsConfig config, long nodeCount) {
        if (config.hasRelationshipWeightProperty()) {
            var totalWeights = HugeAtomicDoubleArray.of(
                nodeCount,
                ParallelDoublePageCreator.passThrough(config.concurrency())
            );
            totalWeights.setAll(TOTAL_WEIGHT_MISSING);
            return Optional.of(totalWeights);
        }
        return Optional.empty();
    }

    private RandomWalkCompanion() {}
}
