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

import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictStreamConfig;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KGE;

public class MachineLearningAlgorithmsStreamModeBusinessFacade {
    private final AlgorithmProcessingTemplateConvenience convenience;

    private final MachineLearningAlgorithmsEstimationModeBusinessFacade estimation;
    private final MachineLearningAlgorithms algorithms;

    MachineLearningAlgorithmsStreamModeBusinessFacade(
        AlgorithmProcessingTemplateConvenience convenience,
        MachineLearningAlgorithmsEstimationModeBusinessFacade estimation,
        MachineLearningAlgorithms algorithms
    ) {
        this.convenience = convenience;
        this.algorithms = algorithms;
        this.estimation = estimation;
    }

    public <RESULT> Stream<RESULT> kge(
        GraphName graphName,
        KGEPredictStreamConfig configuration,
        StreamResultBuilder<KGEPredictResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            KGE,
            estimation::kge,
            (graph, __) -> algorithms.kge(graph, configuration),
            resultBuilder
        );
    }
}
