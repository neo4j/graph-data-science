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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.logging.Log;

public final class MiscellaneousApplications {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimation;
    private final MiscellaneousApplicationsMutateModeBusinessFacade mutation;
    private final MiscellaneousApplicationsStatsModeBusinessFacade stats;
    private final MiscellaneousApplicationsStreamModeBusinessFacade stream;
    private final MiscellaneousApplicationsWriteModeBusinessFacade write;

    private MiscellaneousApplications(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimation,
        MiscellaneousApplicationsMutateModeBusinessFacade mutation,
        MiscellaneousApplicationsStatsModeBusinessFacade stats,
        MiscellaneousApplicationsStreamModeBusinessFacade stream,
        MiscellaneousApplicationsWriteModeBusinessFacade write
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
        this.stats = stats;
        this.stream = stream;
        this.write = write;
    }

    public static MiscellaneousApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty
    ) {
        var algorithms = new MiscellaneousAlgorithms(
            progressTrackerCreator,
            requestScopedDependencies.terminationFlag()
        );

        var estimation = new MiscellaneousApplicationsEstimationModeBusinessFacade(algorithmEstimationTemplate);
        var mutation = new MiscellaneousApplicationsMutateModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodeProperty
        );
        var stats = new MiscellaneousApplicationsStatsModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var stream = new MiscellaneousApplicationsStreamModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var writeToDatabase = new WriteToDatabase(log, requestScopedDependencies, writeContext);
        var write = new MiscellaneousApplicationsWriteModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience,
            writeToDatabase
        );

        return new MiscellaneousApplications(estimation, mutation, stats, stream, write);
    }

    public MiscellaneousApplicationsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public MiscellaneousApplicationsMutateModeBusinessFacade mutate() {
        return mutation;
    }

    public MiscellaneousApplicationsStatsModeBusinessFacade stats() {
        return stats;
    }

    public MiscellaneousApplicationsStreamModeBusinessFacade stream() {
        return stream;
    }

    public MiscellaneousApplicationsWriteModeBusinessFacade write() {
        return write;
    }
}
