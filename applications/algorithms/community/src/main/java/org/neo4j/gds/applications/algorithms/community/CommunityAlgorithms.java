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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.List;

public class CommunityAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public CommunityAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    ApproxMaxKCutResult approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        var task = Tasks.iterativeFixed(
            LabelForProgressTracking.ApproximateMaximumKCut.value,
            () -> List.of(
                Tasks.leaf("place nodes randomly", graph.nodeCount()),
                searchTask(graph.nodeCount(), configuration.vnsMaxNeighborhoodOrder())
            ),
            configuration.iterations()
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = ApproxMaxKCut.create(
            graph,
            configuration.toParameters(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    ConductanceResult conductance(Graph graph, ConductanceStreamConfig configuration) {
        var task = Tasks.task(
            LabelForProgressTracking.Conductance.value,
            Tasks.leaf("count relationships", graph.nodeCount()),
            Tasks.leaf("accumulate counts"),
            Tasks.leaf("perform conductance computations")
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var algorithm = new Conductance(
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    DisjointSetStruct wcc(Graph graph, WccBaseConfig configuration) {
        var task = Tasks.leaf(LabelForProgressTracking.WCC.value, graph.relationshipCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            progressTracker.logWarning(
                "Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }

        var algorithm = new Wcc(
            graph,
            DefaultPool.INSTANCE,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            configuration.toParameters(),
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    private static Task searchTask(long nodeCount, int vnsMaxNeighborhoodOrder) {
        if (vnsMaxNeighborhoodOrder > 0) {
            return Tasks.iterativeOpen(
                "variable neighborhood search",
                () -> List.of(localSearchTask(nodeCount))
            );
        }

        return localSearchTask(nodeCount);
    }

    private static Task localSearchTask(long nodeCount) {
        return Tasks.task(
            "local search",
            Tasks.iterativeOpen(
                "improvement loop",
                () -> List.of(
                    Tasks.leaf("compute node to community weights", nodeCount),
                    Tasks.leaf("swap for local improvements", nodeCount)
                )
            ),
            Tasks.leaf("compute current solution cost", nodeCount)
        );
    }
}
