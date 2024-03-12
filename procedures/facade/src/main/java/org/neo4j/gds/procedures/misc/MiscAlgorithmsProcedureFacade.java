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
package org.neo4j.gds.procedures.misc;

import org.neo4j.gds.algorithms.misc.MiscAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesStreamResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;

import java.util.Map;
import java.util.stream.Stream;

public class MiscAlgorithmsProcedureFacade {
    // services
    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;

    // business logic
    private final MiscAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final MiscAlgorithmStreamBusinessFacade streamBusinessFacade;

    public MiscAlgorithmsProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        MiscAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        MiscAlgorithmStreamBusinessFacade streamBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
    }

    public Stream<ScalePropertiesStreamResult> scalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
       ){

        var config = configurationCreator.createConfigurationForStream(configuration, ScalePropertiesStreamConfig::of);

        var streamResult = streamBusinessFacade.scaleProperties(graphName, config);
        return ScalePropertiesComputationResultTransformer.toStreamResult(streamResult);
    }

    public Stream<ScalePropertiesStreamResult> alphaScalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ){

        var config = configurationCreator.createConfigurationForStream(configuration, ScalePropertiesStreamConfig::of);

        var streamResult = streamBusinessFacade.alphaScaleProperties(graphName, config);
        return ScalePropertiesComputationResultTransformer.toStreamResult(streamResult);
    }


}
