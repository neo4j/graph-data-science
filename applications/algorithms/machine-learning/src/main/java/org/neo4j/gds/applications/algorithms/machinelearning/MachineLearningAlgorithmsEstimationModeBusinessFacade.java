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
package org.neo4j.gds.applications.algorithms.machinelearning;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.ml.splitting.SplitRelationshipConfigTransformer;
import org.neo4j.gds.ml.splitting.SplitRelationshipsBaseConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsEstimateDefinition;

public class MachineLearningAlgorithmsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    MachineLearningAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimation kge() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation splitRelationships(SplitRelationshipsBaseConfig configuration) {
        var parameters = SplitRelationshipConfigTransformer.toMemoryEstimateParameters(configuration);

        return new SplitRelationshipsEstimateDefinition(parameters).memoryEstimation();
    }

    public MemoryEstimateResult splitRelationships(
        SplitRelationshipsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = splitRelationships(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }
}
