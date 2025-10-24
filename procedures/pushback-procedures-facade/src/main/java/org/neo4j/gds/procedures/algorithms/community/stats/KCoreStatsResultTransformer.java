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

import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.procedures.algorithms.community.KCoreDecompositionStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class KCoreStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<KCoreDecompositionResult>, Stream<KCoreDecompositionStatsResult>> {

    private final Map<String, Object> configuration;

    public KCoreStatsResultTransformer(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<KCoreDecompositionStatsResult> apply(TimedAlgorithmResult<KCoreDecompositionResult> timedAlgorithmResult) {

        var kCoreDecompositionResult = timedAlgorithmResult.result();

        var k1ColoringStatsResult =  new KCoreDecompositionStatsResult(
            kCoreDecompositionResult.degeneracy(),
            0,
            timedAlgorithmResult.computeMillis(),
            0,
            configuration
        );

        return Stream.of(k1ColoringStatsResult);

    }
}
