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
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.logging.Log;

public final class CommunityApplications {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CommunityAlgorithmsMutateModeBusinessFacade mutation;
    private final CommunityAlgorithmsStatsModeBusinessFacade stats;
    private final CommunityAlgorithmsStreamModeBusinessFacade stream;
    private final CommunityAlgorithmsWriteModeBusinessFacade write;

    private CommunityApplications(
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithmsMutateModeBusinessFacade mutation,
        CommunityAlgorithmsStatsModeBusinessFacade stats,
        CommunityAlgorithmsStreamModeBusinessFacade stream,
        CommunityAlgorithmsWriteModeBusinessFacade write
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
        this.stats = stats;
        this.stream = stream;
        this.write = write;
    }

    static CommunityApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty
    ) {
        var estimation = new CommunityAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);
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
        var stats = new CommunityAlgorithmsStatsModeBusinessFacade(estimation, algorithms, algorithmProcessingTemplate);
        var stream = new CommunityAlgorithmsStreamModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplate
        );
        var write = CommunityAlgorithmsWriteModeBusinessFacade.create(
            log,
            requestScopedDependencies,
            estimation,
            algorithms,
            algorithmProcessingTemplate
        );

        return new CommunityApplications(estimation, mutation, stats, stream, write);
    }

    public CommunityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public CommunityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutation;
    }

    public CommunityAlgorithmsStatsModeBusinessFacade stats() {
        return stats;
    }

    public CommunityAlgorithmsStreamModeBusinessFacade stream() {
        return stream;
    }

    public CommunityAlgorithmsWriteModeBusinessFacade write() {
        return write;
    }
}
