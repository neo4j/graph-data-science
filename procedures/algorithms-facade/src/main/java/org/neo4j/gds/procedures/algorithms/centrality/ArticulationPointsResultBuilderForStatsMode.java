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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.articulationpoints.ArticulationPointsStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class ArticulationPointsResultBuilderForStatsMode implements StatsResultBuilder<ArticulationPointsStatsConfig, BitSet, Stream<ArticulationPointsStatsResult>> {

    @Override
    public Stream<ArticulationPointsStatsResult> build(
        Graph graph,
        ArticulationPointsStatsConfig configuration,
        Optional<BitSet> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) {
            return Stream.of(ArticulationPointsStatsResult.EMPTY);
        }

        var bitSet = result.get();
        return Stream.of(
            new ArticulationPointsStatsResult(
                    bitSet.cardinality(),
                    timings.computeMillis,
                    configuration.toMap()
            )
        );
    }
}
