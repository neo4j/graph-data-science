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

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.ToMapConvertible;

import java.util.Optional;

class GenericCentralityResultBuilderForWriteMode {
    private final CentralityDistributionComputer centralityDistributionComputer = new CentralityDistributionComputer();

    <CONFIGURATION extends ConcurrencyConfig & ToMapConvertible> CentralityWriteResult build(
        IdMap idMap,
        CONFIGURATION configuration,
        Optional<? extends CentralityAlgorithmResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> nodePropertiesWritten,
        boolean shouldComputeCentralityDistribution
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return CentralityWriteResult.emptyFrom(timings, configurationMap);

        var centralityDistributionAndTiming = centralityDistributionComputer.compute(
            idMap,
            result.get().centralityScoreProvider(),
            configuration,
            shouldComputeCentralityDistribution
        );

        return new CentralityWriteResult(
            nodePropertiesWritten.orElseThrow().value(),
            timings.preProcessingMillis,
            timings.computeMillis,
            centralityDistributionAndTiming.getRight(),
            timings.sideEffectMillis,
            centralityDistributionAndTiming.getLeft(),
            configurationMap
        );
    }
}
