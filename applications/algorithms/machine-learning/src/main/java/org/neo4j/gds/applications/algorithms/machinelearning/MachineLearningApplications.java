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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;

public final class MachineLearningApplications {
    private final MachineLearningAlgorithmsEstimationModeBusinessFacade estimation;
    private final MachineLearningAlgorithmsMutateModeBusinessFacade mutation;
    private final MachineLearningAlgorithmsStreamModeBusinessFacade streaming;

    private MachineLearningApplications(
        MachineLearningAlgorithmsEstimationModeBusinessFacade estimation,
        MachineLearningAlgorithmsMutateModeBusinessFacade mutation,
        MachineLearningAlgorithmsStreamModeBusinessFacade streaming
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
        this.streaming = streaming;
    }

    public static MachineLearningApplications create(
        RequestScopedDependencies requestScopedDependencies,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmProcessingTemplateConvenience convenience
    ) {
        var algorithms = new MachineLearningAlgorithms(
            progressTrackerCreator,
            requestScopedDependencies.getTerminationFlag()
        );

        var estimation = new MachineLearningAlgorithmsEstimationModeBusinessFacade();
        var mutation = new MachineLearningAlgorithmsMutateModeBusinessFacade(
            requestScopedDependencies,
            estimation,
            algorithms,
            convenience
        );
        var streaming = new MachineLearningAlgorithmsStreamModeBusinessFacade(
            convenience,
            estimation,
            algorithms
        );

        return new MachineLearningApplications(estimation, mutation, streaming);
    }

    public MachineLearningAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public MachineLearningAlgorithmsMutateModeBusinessFacade mutate() {
        return mutation;
    }

    public MachineLearningAlgorithmsStreamModeBusinessFacade stream() {
        return streaming;
    }
}
