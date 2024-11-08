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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.hits.HitsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class HitsResultBuilderForStatsMode implements StatsResultBuilder<PregelResult, Stream<HitsStatsResult>> {

    private final HitsConfig configuration;

    HitsResultBuilderForStatsMode(HitsConfig configuration) {
        this.configuration = configuration;
    }
    @Override
    public Stream<HitsStatsResult> build(
        Graph graph,
        Optional<PregelResult> pregelResult,
        AlgorithmProcessingTimings timings
    ) {
        return Stream.of(
            pregelResult
                .map( result-> new HitsStatsResult(result.ranIterations(), result.didConverge(), timings.preProcessingMillis,timings.computeMillis,configuration.toMap()))
            .orElse(HitsStatsResult.emptyFrom(timings,configuration.toMap())));

    }
}
