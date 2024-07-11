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

import org.neo4j.gds.algorithms.misc.MiscAlgorithmWriteBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesWriteResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class MiscAlgorithmsProcedureFacade {
    // services
    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;

    // business logic
    private final MiscAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final MiscAlgorithmWriteBusinessFacade writeBusinessFacade;


    public MiscAlgorithmsProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        MiscAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        MiscAlgorithmWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
    }

    public Stream<ScalePropertiesWriteResult> scalePropertiesWrite(
        String graphName,
        Map<String, Object> configuration
    ) {

        var config = configurationCreator.createConfiguration(configuration, ScalePropertiesWriteConfig::of, Optional.empty());
        var returnStatistics = procedureReturnColumns.contains("scalerStatistics");
        var writeResult = writeBusinessFacade.scaleProperties(graphName, config);
        return Stream.of(ScalePropertiesComputationResultTransformer.toWriteResult(
            writeResult,
            returnStatistics
        ));
    }

    public Stream<MemoryEstimateResult> scalePropertiesWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ScalePropertiesWriteConfig::of, Optional.empty());
        return Stream.of(estimateBusinessFacade.scaleProperties(graphNameOrConfiguration, config));
    }


}
