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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class K1ColoringResultBuilderForStatsMode implements StatsResultBuilder<K1ColoringResult, Stream<K1ColoringStatsResult>> {
    private final K1ColoringStatsConfig configuration;
    private final boolean computeUsedColors;

    K1ColoringResultBuilderForStatsMode(K1ColoringStatsConfig configuration, boolean computeUsedColors) {
        this.configuration = configuration;
        this.computeUsedColors = computeUsedColors;
    }

    @Override
    public Stream<K1ColoringStatsResult> build(
        Graph graph,
        Optional<K1ColoringResult> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return K1ColoringStatsResult.emptyFrom(timings, configuration.toMap());

        var k1ColoringResult = result.get();

        var usedColors = (computeUsedColors) ? k1ColoringResult.usedColors().cardinality() : 0;

        var k1ColoringStatsResult = new K1ColoringStatsResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            k1ColoringResult.colors().size(),
            usedColors,
            k1ColoringResult.ranIterations(),
            k1ColoringResult.didConverge(),
            configuration.toMap()
        );

        return Stream.of(k1ColoringStatsResult);
    }
}
