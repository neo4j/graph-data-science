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
package org.neo4j.gds.applications;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithms;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.logging.Log;

public final class SimilarityApplications {
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeFacade;
    private final SimilarityAlgorithmsMutateModeBusinessFacade mutateModeFacade;

    private SimilarityApplications(
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeFacade,
        SimilarityAlgorithmsMutateModeBusinessFacade mutateModeFacade
    ) {
        this.estimationModeFacade = estimationModeFacade;
        this.mutateModeFacade = mutateModeFacade;
    }

    static SimilarityApplications create(
        Log log,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        var estimationModeFacade = new SimilarityAlgorithmsEstimationModeBusinessFacade();
        var similarityAlgorithms = new SimilarityAlgorithms(progressTrackerCreator);

        var mutateModeFacade = new SimilarityAlgorithmsMutateModeBusinessFacade(
            log,
            estimationModeFacade,
            similarityAlgorithms,
            algorithmProcessingTemplate
        );

        return new SimilarityApplications(estimationModeFacade, mutateModeFacade);
    }

    public SimilarityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimationModeFacade;
    }

    public SimilarityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutateModeFacade;
    }
}
