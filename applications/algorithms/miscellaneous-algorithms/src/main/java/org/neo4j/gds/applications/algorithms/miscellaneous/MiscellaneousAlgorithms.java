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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;

public class MiscellaneousAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;

    MiscellaneousAlgorithms(ProgressTrackerCreator progressTrackerCreator) {
        this.progressTrackerCreator = progressTrackerCreator;
    }

    ScalePropertiesResult scaleProperties(Graph graph, ScalePropertiesBaseConfig configuration) {
        int totalPropertyDimension = configuration
            .nodeProperties()
            .stream()
            .map(graph::nodeProperties)
            .mapToInt(p -> p.dimension().orElseThrow(/* already validated in config */))
            .sum();
        var task = Tasks.task(
            LabelForProgressTracking.ScaleProperties.value,
            Tasks.leaf("Prepare scalers", graph.nodeCount() * totalPropertyDimension),
            Tasks.leaf("Scale properties", graph.nodeCount() * totalPropertyDimension)
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new ScaleProperties(
            graph,
            configuration,
            progressTracker,
            DefaultPool.INSTANCE
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }
}
