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

import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.core.utils.logging.GdsLoggers;

public final class CentralityApplications {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CentralityAlgorithmsMutateModeBusinessFacade mutation;
    private final InstrumentedCentralityAlgorithms raw;
    private final CentralityAlgorithmsStatsModeBusinessFacade stats;
    private final CentralityAlgorithmsStreamModeBusinessFacade streaming;
    private final CentralityAlgorithmsWriteModeBusinessFacade writing;

    private CentralityApplications(
        CentralityAlgorithmsEstimationModeBusinessFacade estimation,
        CentralityAlgorithmsMutateModeBusinessFacade mutation,
        InstrumentedCentralityAlgorithms raw,
        CentralityAlgorithmsStatsModeBusinessFacade stats,
        CentralityAlgorithmsStreamModeBusinessFacade streaming,
        CentralityAlgorithmsWriteModeBusinessFacade writing
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
        this.raw = raw;
        this.stats = stats;
        this.streaming = streaming;
        this.writing = writing;
    }

    public static CentralityApplications create(
        GdsLoggers loggers,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate estimationTemplate,
        LaunchConvenience launchConvenience,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodePropertyService mutateNodePropertyService,
        Synchroniser synchroniser
    ) {
        var algorithms = new CentralityAlgorithms(requestScopedDependencies.terminationFlag());
        var trackedAlgorithms = new TrackedCentralityAlgorithms(algorithms, progressTrackerCreator);
        var estimation = new CentralityAlgorithmsEstimationModeBusinessFacade(estimationTemplate);
        var raw = InstrumentedCentralityAlgorithms.create(
            trackedAlgorithms,
            estimation,
            launchConvenience,
            progressTrackerCreator,
            requestScopedDependencies.terminationFlag()
        );

        var mutation = new CentralityAlgorithmsMutateModeBusinessFacade(
            estimation,
            trackedAlgorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodePropertyService,
            raw,
            synchroniser
        );

        var stats = new CentralityAlgorithmsStatsModeBusinessFacade(
            estimation,
            trackedAlgorithms,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );

        var streaming = new CentralityAlgorithmsStreamModeBusinessFacade(
            estimation,
            trackedAlgorithms,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );

        var writing = CentralityAlgorithmsWriteModeBusinessFacade.create(
            loggers.log(),
            requestScopedDependencies,
            writeContext,
            estimation,
            trackedAlgorithms,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );

        return new CentralityApplications(estimation, mutation, raw, stats, streaming, writing);
    }

    public CentralityAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public CentralityAlgorithmsMutateModeBusinessFacade mutate() {
        return mutation;
    }

    /**
     * Raw as in, no mode applied, just access to the asynchronous machinery
     */
    public InstrumentedCentralityAlgorithms raw() {
        return raw;
    }

    public CentralityAlgorithmsStatsModeBusinessFacade stats() {
        return stats;
    }

    public CentralityAlgorithmsStreamModeBusinessFacade stream() {
        return streaming;
    }

    public CentralityAlgorithmsWriteModeBusinessFacade write() {
        return writing;
    }
}
