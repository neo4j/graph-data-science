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

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.k1coloring.K1Coloring;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecomposition;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.ImmutableKmeansContext;
import org.neo4j.gds.kmeans.Kmeans;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagation;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.Leiden;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.List;
import java.util.Optional;

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

    ConductanceResult conductance(Graph graph, ConductanceBaseConfig configuration) {
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

    K1ColoringResult k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        var task = Tasks.iterativeDynamic(
            LabelForProgressTracking.K1Coloring.value,
            () -> List.of(
                Tasks.leaf("color nodes", graph.nodeCount()),
                Tasks.leaf("validate nodes", graph.nodeCount())
            ),
            configuration.maxIterations()
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var algorithm = new K1Coloring(
            graph,
            parameters.maxIterations(),
            parameters.batchSize(),
            parameters.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    KCoreDecompositionResult kCore(Graph graph, AlgoBaseConfig configuration) {
        var task = Tasks.leaf(LabelForProgressTracking.KCore.value, graph.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new KCoreDecomposition(graph, configuration.concurrency(), progressTracker, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    KmeansResult kMeans(Graph graph, KmeansBaseConfig configuration) {
        var seedCentroids = (List) configuration.seedCentroids();
        if (configuration.numberOfRestarts() > 1 && seedCentroids.size() > 0) {
            throw new IllegalArgumentException("K-Means cannot be run multiple time when seeded");
        }
        if (seedCentroids.size() > 0 && seedCentroids.size() != configuration.k()) {
            throw new IllegalArgumentException("Incorrect number of seeded centroids given for running K-Means");
        }

        var task = constructKMeansProgressTask(graph, configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var kmeansContext = ImmutableKmeansContext.builder()
            .executor(DefaultPool.INSTANCE)
            .progressTracker(progressTracker)
            .build();
        var algorithm = Kmeans.createKmeans(graph, configuration.toParameters(), kmeansContext, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    LabelPropagationResult labelPropagation(Graph graph, LabelPropagationBaseConfig configuration) {
        var task = Tasks.task(
            LabelForProgressTracking.LabelPropagation.value,
            Tasks.leaf("Initialization", graph.relationshipCount()),
            Tasks.iterativeDynamic(
                "Assign labels",
                () -> List.of(Tasks.leaf("Iteration", graph.relationshipCount())),
                configuration.maxIterations()
            )
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new LabelPropagation(
            graph,
            configuration.toParameters(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    LeidenResult leiden(Graph graph, LeidenMutateConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The Leiden algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var iterations = configuration.maxLevels();
        var iterativeTasks = Tasks.iterativeDynamic(
            "Iteration",
            () ->
                List.of(
                    Tasks.leaf("Local Move", 1),
                    Tasks.leaf("Modularity Computation", graph.nodeCount()),
                    Tasks.leaf("Refinement", graph.nodeCount()),
                    Tasks.leaf("Aggregation", graph.nodeCount())
                ),
            iterations
        );
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var task = Tasks.task(LabelForProgressTracking.Leiden.value, initializationTask, iterativeTasks);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();
        var seedValues = Optional.ofNullable(parameters.seedProperty())
            .map(seedParameter -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedParameter))
            .orElse(null);

        var algorithm = new Leiden(
            graph,
            parameters.maxLevels(),
            parameters.gamma(),
            parameters.theta(),
            parameters.includeIntermediateCommunities(),
            parameters.randomSeed().orElse(0L),
            seedValues,
            parameters.tolerance(),
            parameters.concurrency(),
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

    private Task constructKMeansProgressTask(Graph graph, KmeansBaseConfig configuration) {
        var label = LabelForProgressTracking.KMeans;

        var iterations = configuration.numberOfRestarts();
        if (iterations == 1) {
            return kMeansTask(graph, label.value, configuration);
        }

        return Tasks.iterativeFixed(
            label.value,
            () -> List.of(kMeansTask(graph, "KMeans Iteration", configuration)),
            iterations
        );
    }

    private Task kMeansTask(Graph graph, String description, KmeansBaseConfig configuration) {
        if (configuration.computeSilhouette()) {
            return Tasks.task(description, List.of(
                Tasks.leaf("Initialization", configuration.k()),
                Tasks.iterativeDynamic("Main", () -> List.of(Tasks.leaf("Iteration")), configuration.maxIterations()),
                Tasks.leaf("Silhouette", graph.nodeCount())

            ));
        } else {
            return Tasks.task(description, List.of(
                Tasks.leaf("Initialization", configuration.k()),
                Tasks.iterativeDynamic("Main", () -> List.of(Tasks.leaf("Iteration")), configuration.maxIterations())
            ));
        }
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
