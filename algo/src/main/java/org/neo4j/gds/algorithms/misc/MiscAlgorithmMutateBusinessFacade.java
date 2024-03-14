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

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;

public class MiscAlgorithmMutateBusinessFacade {
    private final MiscAlgorithmsFacade miscAlgorithmsFacade;
    private final MutateNodePropertyService mutateNodePropertyService;

    public MiscAlgorithmMutateBusinessFacade(MiscAlgorithmsFacade miscAlgorithmsFacade,MutateNodePropertyService mutateNodePropertyService) {
        this.miscAlgorithmsFacade = miscAlgorithmsFacade;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public NodePropertyMutateResult<ScalePropertiesSpecificFields> scaleProperties(
        String graphName,
        ScalePropertiesMutateConfig configuration
    ) {
        return  scaleProperties(graphName,configuration,false);
    }

    public NodePropertyMutateResult<ScalePropertiesSpecificFields> alphaScaleProperties(
        String graphName,
        ScalePropertiesMutateConfig configuration
        ) {
        return  scaleProperties(graphName,configuration,true);
    }



    public NodePropertyMutateResult<ScalePropertiesSpecificFields> scaleProperties(
        String graphName,
        ScalePropertiesMutateConfig configuration,
        boolean allowL1L2
    ) {

        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> miscAlgorithmsFacade.scaleProperties(graphName, configuration,allowL1L2)
        );

        var mutateResultBuilder = NodePropertyMutateResult.<ScalePropertiesSpecificFields>builder()
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
                var mutateResult = mutateNodePropertyService.mutate(
                    configuration.mutateProperty(),
                    nodeProperties,
                    configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                    algorithmResult.graph(),
                    algorithmResult.graphStore()
                );
                mutateResultBuilder.mutateMillis(mutateResult.mutateMilliseconds());
                mutateResultBuilder.nodePropertiesWritten(mutateResult.nodePropertiesAdded());
                mutateResultBuilder.algorithmSpecificFields(new ScalePropertiesSpecificFields(result.scalerStatistics()));
            },
            () -> mutateResultBuilder.algorithmSpecificFields(ScalePropertiesSpecificFields.EMPTY)
        );

        return mutateResultBuilder.build();
    }


}
