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
package org.neo4j.gds.procedures.algorithms.centrality.stubs;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityDistributionComputer;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityMutateResult;

import java.util.Optional;

class GenericCentralityResultBuilderForMutateMode {
    private final CentralityDistributionComputer centralityDistributionComputer = new CentralityDistributionComputer();

    <CONFIGURATION extends ConcurrencyConfig & ToMapConvertible> CentralityMutateResult build(
        IdMap graph,
        CONFIGURATION configuration,
        Optional<? extends CentralityAlgorithmResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata,
        boolean shouldComputeCentralityDistribution
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return CentralityMutateResult.emptyFrom(timings, configurationMap);

        var centralityResult = result.get();

        var centralityDistributionAndTiming = centralityDistributionComputer.compute(
            graph,
            centralityResult.centralityScoreProvider(),
            configuration,
            shouldComputeCentralityDistribution
        );

        return new CentralityMutateResult(
            metadata.orElseThrow().value(),
            timings.preProcessingMillis,
            timings.computeMillis,
            centralityDistributionAndTiming.getRight(),
            timings.mutateOrWriteMillis,
            centralityDistributionAndTiming.getLeft(),
            configurationMap
        );
    }
}
