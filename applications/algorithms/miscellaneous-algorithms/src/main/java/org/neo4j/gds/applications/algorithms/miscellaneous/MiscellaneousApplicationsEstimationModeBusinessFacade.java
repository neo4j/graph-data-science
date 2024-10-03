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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.indexInverse.InverseRelationshipsMemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesMemoryEstimateDefinition;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.undirected.ToUndirectedConfigTransformer;
import org.neo4j.gds.undirected.ToUndirectedMemoryEstimateDefinition;

public class MiscellaneousApplicationsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    MiscellaneousApplicationsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimation collapsePath() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation indexInverse(InverseRelationshipsConfig configuration) {
        return new InverseRelationshipsMemoryEstimateDefinition(configuration.relationshipTypes()).memoryEstimation();
    }

    public MemoryEstimation scaleProperties(ScalePropertiesBaseConfig configuration) {
        return new ScalePropertiesMemoryEstimateDefinition(configuration.nodeProperties()).memoryEstimation();
    }

    public MemoryEstimateResult scaleProperties(
        ScalePropertiesBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = scaleProperties(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation toUndirected(ToUndirectedConfig configuration) {
        return new ToUndirectedMemoryEstimateDefinition(ToUndirectedConfigTransformer.toMemoryEstimateParameters(configuration)).memoryEstimation();
    }
}
