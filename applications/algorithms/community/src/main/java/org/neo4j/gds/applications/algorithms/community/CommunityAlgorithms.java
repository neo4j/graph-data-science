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
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.k1coloring.K1Coloring;
import org.neo4j.gds.k1coloring.K1ColoringAlgorithmFactory;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfigImpl;
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
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.Louvain;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculator;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimization;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationFactory;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.Scc;
import org.neo4j.gds.scc.SccCommonBaseConfig;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.IntersectingTriangleCount;
import org.neo4j.gds.triangle.IntersectingTriangleCountFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleStream;
import org.neo4j.gds.triangle.TriangleStreamResult;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.modularityoptimization.ModularityOptimization.K1COLORING_MAX_ITERATIONS;

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
            Algorithm.ApproximateMaximumKCut.labelForProgressTracking,
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
            Algorithm.Conductance.labelForProgressTracking,
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
            Algorithm.K1Coloring.labelForProgressTracking,
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
        var task = Tasks.leaf(Algorithm.KCore.labelForProgressTracking, graph.nodeCount());
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
            Algorithm.LabelPropagation.labelForProgressTracking,
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

    LocalClusteringCoefficientResult lcc(Graph graph, LocalClusteringCoefficientBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (configuration.seedProperty() == null) {
            tasks.add(IntersectingTriangleCountFactory.triangleCountProgressTask(graph));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", graph.nodeCount()));
        var task = Tasks.task(Algorithm.LCC.labelForProgressTracking, tasks);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var algorithm = new LocalClusteringCoefficient(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            parameters.seedProperty(),
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    LeidenResult leiden(Graph graph, LeidenBaseConfig configuration) {
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
        var task = Tasks.task(Algorithm.Leiden.labelForProgressTracking, initializationTask, iterativeTasks);
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
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    LouvainResult louvain(Graph graph, LouvainBaseConfig configuration) {
        var task = Tasks.iterativeDynamic(
            Algorithm.Louvain.labelForProgressTracking,
            () -> List.of(ModularityOptimizationFactory.progressTask(graph, configuration.maxIterations())),
            configuration.maxLevels()
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var algorithm = new Louvain(
            graph,
            parameters.concurrency(),
            parameters.maxIterations(),
            parameters.tolerance(),
            parameters.maxLevels(),
            parameters.includeIntermediateCommunities(),
            parameters.seedProperty(),
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        var algorithm = ModularityCalculator.create(
            graph,
            graph.nodeProperties(configuration.communityProperty())::longValue,
            configuration.concurrency()
        );

        return algorithm.compute();
    }

    ModularityOptimizationResult modularityOptimization(Graph graph, ModularityOptimizationBaseConfig configuration) {
        var task = Tasks.task(
            Algorithm.ModularityOptimization.labelForProgressTracking,
            Tasks.task(
                "initialization",
                K1ColoringAlgorithmFactory.k1ColoringProgressTask(graph, createModularityConfig())
            ),
            Tasks.iterativeDynamic(
                "compute modularity",
                () -> List.of(Tasks.leaf("optimizeForColor", graph.relationshipCount())),
                configuration.maxIterations()
            )
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var seedPropertyValues = configuration.seedProperty() != null ?
            CommunityCompanion.extractSeedingNodePropertyValues(
                graph,
                configuration.seedProperty()
            ) : null;

        var parameters = configuration.toParameters();

        var algorithm = new ModularityOptimization(
            graph,
            parameters.maxIterations(),
            parameters.tolerance(),
            seedPropertyValues,
            parameters.concurrency(),
            parameters.batchSize(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    HugeLongArray scc(Graph graph, SccCommonBaseConfig configuration) {
        var progressTracker = progressTrackerCreator.createProgressTracker(
            configuration,
            Tasks.leaf(Algorithm.SCC.labelForProgressTracking, graph.nodeCount())
        );

        var algorithm = new Scc(graph, progressTracker, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    TriangleCountResult triangleCount(Graph graph, TriangleCountBaseConfig configuration) {
        var task = Tasks.leaf(Algorithm.TriangleCount.labelForProgressTracking, graph.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var algorithm = IntersectingTriangleCount.create(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    Stream<TriangleStreamResult> triangles(Graph graph, ConcurrencyConfig configuration) {
        var algorithm = TriangleStream.create(
            graph,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            terminationFlag
        );

        return algorithm.compute();
    }

    DisjointSetStruct wcc(Graph graph, WccBaseConfig configuration) {
        var task = Tasks.leaf(Algorithm.WCC.labelForProgressTracking, graph.relationshipCount());
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
        var label = Algorithm.KMeans.labelForProgressTracking;

        var iterations = configuration.numberOfRestarts();
        if (iterations == 1) {
            return kMeansTask(graph, label, configuration);
        }

        return Tasks.iterativeFixed(
            label,
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

    private K1ColoringBaseConfig createModularityConfig() {
        return K1ColoringStreamConfigImpl
            .builder()
            .maxIterations(K1COLORING_MAX_ITERATIONS)
            .build();

    }
}
