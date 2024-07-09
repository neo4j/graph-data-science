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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;

public final class MiscellaneousApplications {
    private final MiscellaneousEstimationModeBusinessFacade estimation;
    private final MiscellaneousMutateModeBusinessFacade mutation;

    private MiscellaneousApplications(
        MiscellaneousEstimationModeBusinessFacade estimation,
        MiscellaneousMutateModeBusinessFacade mutation
    ) {
        this.estimation = estimation;
        this.mutation = mutation;
    }

    public static MiscellaneousApplications create(
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty
    ) {
        var algorithms = new MiscellaneousAlgorithms(progressTrackerCreator);

        var estimation = new MiscellaneousEstimationModeBusinessFacade();
        var mutation = new MiscellaneousMutateModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodeProperty
        );

        return new MiscellaneousApplications(
            estimation,
            mutation
        );
    }

    public MiscellaneousEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public MiscellaneousMutateModeBusinessFacade mutate() {
        return mutation;
    }
}
