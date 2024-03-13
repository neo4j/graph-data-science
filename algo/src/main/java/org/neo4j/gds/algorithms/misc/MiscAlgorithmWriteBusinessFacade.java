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

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

public class MiscAlgorithmWriteBusinessFacade {
    private final MiscAlgorithmsFacade miscAlgorithmsFacade;
    private final WriteNodePropertyService writeNodePropertyService;

    public MiscAlgorithmWriteBusinessFacade(MiscAlgorithmsFacade miscAlgorithmsFacade,WriteNodePropertyService writeNodePropertyService) {
        this.miscAlgorithmsFacade = miscAlgorithmsFacade;
        this.writeNodePropertyService = writeNodePropertyService;
    }

    public NodePropertyWriteResult<ScalePropertiesSpecificFields> scaleProperties(
        String graphName,
        ScalePropertiesWriteConfig configuration
    ) {

        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> miscAlgorithmsFacade.scaleProperties(graphName, configuration,false)
        );

        var writeResultBuilder = NodePropertyWriteResult.<ScalePropertiesSpecificFields>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .configuration(configuration)
            .postProcessingMillis(0);

        var algorithmResult = intermediateResult.algorithmResult;
        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                var nodeProperties = new ScaledPropertiesNodePropertyValues(
                    nodeCount,
                    result.scaledProperties());
                var writeResult = writeNodePropertyService.write(
                    algorithmResult.graph(),
                    algorithmResult.graphStore(),
                    nodeProperties,
                    configuration.writeConcurrency(),
                    configuration.writeProperty(),
                    "CELFWrite",
                    configuration.arrowConnectionInfo()
                );
                writeResultBuilder.writeMillis(writeResult.writeMilliseconds());
                writeResultBuilder.nodePropertiesWritten(writeResult.nodePropertiesWritten());
                writeResultBuilder.algorithmSpecificFields(new ScalePropertiesSpecificFields(result.scalerStatistics()));
            },
            () -> writeResultBuilder.algorithmSpecificFields(ScalePropertiesSpecificFields.EMPTY)
        );

        return writeResultBuilder.build();
    }


}
