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

import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityResult;

class TrackedCommunityAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final CommunityAlgorithms algorithms;

    TrackedCommunityAlgorithms(
        ProgressTrackerCreator progressTrackerCreator,
        CommunityAlgorithms algorithms
    ) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.algorithms = algorithms;
    }

    ConductanceResult conductance(Graph graph, ConductanceBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.conductance(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var conductanceParameters = ConductanceConfigTransformer.toParameters(configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.conductance(graph, conductanceParameters, progressTracker),
            progressTracker,
            true
        );
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        return algorithms.modularity(graph, configuration.toParameters());
    }
}
