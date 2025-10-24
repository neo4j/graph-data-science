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
package org.neo4j.gds.procedures.algorithms.community.stats;

import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class K1ColoringStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<K1ColoringResult>, Stream<K1ColoringStatsResult>> {

    private final Map<String, Object> configuration;
    private final boolean computeUsedColors;

    public K1ColoringStatsResultTransformer(Map<String, Object> configuration, boolean computeUsedColors) {
        this.configuration = configuration;
        this.computeUsedColors = computeUsedColors;
    }

    @Override
    public Stream<K1ColoringStatsResult> apply(TimedAlgorithmResult<K1ColoringResult> timedAlgorithmResult) {

        var k1ColoringResult = timedAlgorithmResult.result();

        var usedColors = (computeUsedColors) ? k1ColoringResult.usedColors().cardinality() : 0;

        var k1ColoringStatsResult =  new K1ColoringStatsResult(
            0,
            timedAlgorithmResult.computeMillis(),
            k1ColoringResult.colors().size(),
            usedColors,
            k1ColoringResult.ranIterations(),
            k1ColoringResult.didConverge(),
            configuration
        );

        return Stream.of(k1ColoringStatsResult);

    }
}
