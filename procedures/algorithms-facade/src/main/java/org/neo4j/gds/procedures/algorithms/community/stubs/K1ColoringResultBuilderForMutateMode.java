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
package org.neo4j.gds.procedures.algorithms.community.stubs;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringMutateResult;

import java.util.Optional;

public class K1ColoringResultBuilderForMutateMode implements ResultBuilder<K1ColoringMutateConfig, K1ColoringResult, K1ColoringMutateResult, Void> {
    private final boolean computeUsedColors;

    public K1ColoringResultBuilderForMutateMode(boolean computeUsedColors) {
        this.computeUsedColors = computeUsedColors;
    }

    @Override
    public K1ColoringMutateResult build(
        Graph graph,
        K1ColoringMutateConfig configuration,
        Optional<K1ColoringResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> metadata
    ) {
        if (result.isEmpty()) return K1ColoringMutateResult.emptyFrom(timings, configuration.toMap());

        var k1ColoringResult = result.get();

        long usedColors = (computeUsedColors) ? k1ColoringResult.usedColors().cardinality() : 0;

        return new K1ColoringMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            k1ColoringResult.colors().size(),
            usedColors,
            k1ColoringResult.ranIterations(),
            k1ColoringResult.didConverge(),
            configuration.toMap()
        );
    }
}
