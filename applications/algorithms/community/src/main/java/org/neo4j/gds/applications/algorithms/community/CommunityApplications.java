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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.logging.Log;

public final class CommunityApplications {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CommunityAlgorithmsMutateModeBusinessFacade mutation;
    private final InstrumentedCommunityAlgorithms raw;
    private final CommunityAlgorithmsStatsModeBusinessFacade stats;
    private final CommunityAlgorithmsStreamModeBusinessFacade stream;
    private final CommunityAlgorithmsWriteModeBusinessFacade write;

    private CommunityApplications(
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithmsMutateModeBusinessFacade mutation,
        InstrumentedCommunityAlgorithms raw,
        CommunityAlgorithmsStatsModeBusinessFacade stats,
        CommunityAlgorithmsStreamModeBusinessFacade stream,
        CommunityAlgorithmsWriteModeBusinessFacade write
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
        this.raw = raw;
        this.stats = stats;
        this.stream = stream;
        this.write = write;
    }

    public static CommunityApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        LaunchConvenience launchConvenience,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodePropertyService mutateNodePropertyService,
        Synchroniser synchroniser
    ) {
        var estimation = new CommunityAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);
        var algorithms = new CommunityAlgorithms(log, requestScopedDependencies.terminationFlag());
        var algorithmsBusinessFacade = new CommunityAlgorithmsBusinessFacade(
            log,
            algorithms,
            progressTrackerCreator
        );
        var mutation = new CommunityAlgorithmsMutateModeBusinessFacade(
            estimation,
            algorithmsBusinessFacade,
            algorithmProcessingTemplateConvenience,
            mutateNodePropertyService
        );
        var trackedCommunityAlgorithms = new TrackedCommunityAlgorithms(
            progressTrackerCreator,
            algorithms
        );
        var raw = new InstrumentedCommunityAlgorithms(
            trackedCommunityAlgorithms,
            estimation,
            launchConvenience
        );
        var stats = new CommunityAlgorithmsStatsModeBusinessFacade(
            estimation,
            algorithmsBusinessFacade,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );
        var stream = new CommunityAlgorithmsStreamModeBusinessFacade(
            estimation,
            algorithmsBusinessFacade,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );
        var write = CommunityAlgorithmsWriteModeBusinessFacade.create(
            log,
            requestScopedDependencies,
            writeContext,
            estimation,
            algorithmsBusinessFacade,
            algorithmProcessingTemplateConvenience
        );
        return new CommunityApplications(estimation, mutation, raw, stats, stream, write);
    }

    public CommunityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public CommunityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutation;
    }

    public InstrumentedCommunityAlgorithms raw() {
        return raw;
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
