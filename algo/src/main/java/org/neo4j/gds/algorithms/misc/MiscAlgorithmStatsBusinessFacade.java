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
package org.neo4j.gds.algorithms.misc;

import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;

public class MiscAlgorithmStatsBusinessFacade {
    private final MiscAlgorithmsFacade miscAlgorithmsFacade;

    public MiscAlgorithmStatsBusinessFacade(MiscAlgorithmsFacade miscAlgorithmsFacade) {
        this.miscAlgorithmsFacade = miscAlgorithmsFacade;
    }

    public StatsResult<ScalePropertiesSpecificFields> scaleProperties(
        String graphName,
        ScalePropertiesBaseConfig config
    ) {

        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> miscAlgorithmsFacade.scaleProperties(graphName, config,false)
        );

        var statsResultBuilder = StatsResult.<ScalePropertiesSpecificFields>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0);

        var algorithmResult = intermediateResult.algorithmResult;
        statsResultBuilder.algorithmSpecificFields(
            algorithmResult.result().map(
                    result -> new ScalePropertiesSpecificFields(
                        result.scalerStatistics()
                    ))
                .orElse(
                    ScalePropertiesSpecificFields.EMPTY
                ));

        return statsResultBuilder.build();
    }


}
