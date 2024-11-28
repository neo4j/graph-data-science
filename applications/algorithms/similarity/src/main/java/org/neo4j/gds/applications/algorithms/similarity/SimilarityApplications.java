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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.logging.Log;

public final class SimilarityApplications {
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeFacade;
    private final SimilarityAlgorithmsMutateModeBusinessFacade mutateModeFacade;
    private final SimilarityAlgorithmsStatsModeBusinessFacade statsModeFacade;
    private final SimilarityAlgorithmsStreamModeBusinessFacade streamModeFacade;
    private final SimilarityAlgorithmsWriteModeBusinessFacade writeModeFacade;

    private SimilarityApplications(
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeFacade,
        SimilarityAlgorithmsMutateModeBusinessFacade mutateModeFacade,
        SimilarityAlgorithmsStatsModeBusinessFacade statsModeFacade,
        SimilarityAlgorithmsStreamModeBusinessFacade streamModeFacade,
        SimilarityAlgorithmsWriteModeBusinessFacade writeModeFacade
    ) {
        this.estimationModeFacade = estimationModeFacade;
        this.mutateModeFacade = mutateModeFacade;
        this.streamModeFacade = streamModeFacade;
        this.statsModeFacade = statsModeFacade;
        this.writeModeFacade = writeModeFacade;
    }

    public static SimilarityApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        WriteContext writeContext
    ) {
        var estimationModeFacade = new SimilarityAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);
        var similarityAlgorithms = new SimilarityAlgorithms(progressTrackerCreator, requestScopedDependencies.getTerminationFlag());

        var mutateModeFacade = new SimilarityAlgorithmsMutateModeBusinessFacade(
            log,
            estimationModeFacade,
            similarityAlgorithms,
            algorithmProcessingTemplateConvenience
        );

        var statsModeFacade = new SimilarityAlgorithmsStatsModeBusinessFacade(
            estimationModeFacade,
            similarityAlgorithms,
            algorithmProcessingTemplateConvenience
        );

        var streamModeFacade = new SimilarityAlgorithmsStreamModeBusinessFacade(
            estimationModeFacade,
            similarityAlgorithms,
            algorithmProcessingTemplateConvenience
        );

        var writeRelationshipService = new WriteRelationshipService(log, requestScopedDependencies, writeContext);

        var writeModeFacade = new SimilarityAlgorithmsWriteModeBusinessFacade(
            estimationModeFacade,
            similarityAlgorithms,
            algorithmProcessingTemplateConvenience,
            writeRelationshipService
        );

        return new SimilarityApplications(
            estimationModeFacade,
            mutateModeFacade,
            statsModeFacade,
            streamModeFacade,
            writeModeFacade
        );
    }

    public SimilarityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimationModeFacade;
    }

    public SimilarityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutateModeFacade;
    }

    public SimilarityAlgorithmsStatsModeBusinessFacade stats() {
        return statsModeFacade;
    }

    public SimilarityAlgorithmsStreamModeBusinessFacade stream() {
        return streamModeFacade;
    }

    public SimilarityAlgorithmsWriteModeBusinessFacade write() {
        return writeModeFacade;
    }
}
