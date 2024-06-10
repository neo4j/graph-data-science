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

import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithms;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;

public final class CommunityApplications {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CommunityAlgorithmsMutateModeBusinessFacade mutation;

    private CommunityApplications(
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithmsMutateModeBusinessFacade mutation
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
    }

    static CommunityApplications create(
        RequestScopedDependencies requestScopedDependencies,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty
    ) {
        var estimation = new CommunityAlgorithmsEstimationModeBusinessFacade();
        var algorithms = new CommunityAlgorithms(
            progressTrackerCreator,
            requestScopedDependencies.getTerminationFlag()
        );
        var mutation = new CommunityAlgorithmsMutateModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplate,
            mutateNodeProperty
        );

        return new CommunityApplications(estimation, mutation);
    }

    public CommunityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public CommunityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutation;
    }
}
