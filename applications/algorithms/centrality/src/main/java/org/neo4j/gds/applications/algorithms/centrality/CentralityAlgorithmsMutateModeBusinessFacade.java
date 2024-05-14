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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ClosenessCentrality;

public class CentralityAlgorithmsMutateModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplate template;
    private final MutateNodeProperty mutateNodeProperty;

    public CentralityAlgorithmsMutateModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimation,
        CentralityAlgorithms algorithms,
        AlgorithmProcessingTemplate template,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.template = template;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityMutateConfig configuration,
        ResultBuilder<BetweennessCentralityMutateConfig, BetwennessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new BetweennessCentralityMutateStep(
            mutateNodeProperty,
            configuration
        );

        return template.processAlgorithm(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimation.betweennessCentrality(configuration),
            graph -> algorithms.betweennessCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityMutateConfig configuration,
        ResultBuilder<ClosenessCentralityMutateConfig, ClosenessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ClosenessCentralityMutateStep(
            mutateNodeProperty,
            configuration
        );

        return template.processAlgorithm(
            graphName,
            configuration,
            ClosenessCentrality,
            () -> estimation.closenessCentrality(configuration),
            graph -> algorithms.closenessCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }
}
