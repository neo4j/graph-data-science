package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.function.Function;

public class GeneralRandomWalkTask implements Runnable {

    protected final Graph graph;
    protected final RandomWalk.NextNodeSupplier nextNodeSupplier;
    protected final ProgressTracker progressTracker;
    protected final RandomWalkBaseConfig config;
    protected final RandomWalkSampler sampler;
    protected Function<long[], Boolean> pathConsumer;


    public GeneralRandomWalkTask(
        RandomWalk.NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        RandomWalkBaseConfig config,
        Graph graph,
        long randomSeed,
        ProgressTracker progressTracker
    ) {

        var maxProbability = Math.max(Math.max(1 / config.returnFactor(), 1.0), 1 / config.inOutFactor());
        var normalizedReturnProbability = (1 / config.returnFactor()) / maxProbability;
        var normalizedSameDistanceProbability = 1 / maxProbability;
        var normalizedInOutProbability = (1 / config.inOutFactor()) / maxProbability;

        this.nextNodeSupplier = nextNodeSupplier;
        this.graph = graph;
        this.config = config;
        this.progressTracker = progressTracker;
        this.sampler = new RandomWalkSampler(
            cumulativeWeightSupplier,
            config.walkLength(),
            normalizedReturnProbability,
            normalizedSameDistanceProbability,
            normalizedInOutProbability,
            graph,
            randomSeed
        );

    }

    public void withPathConsumer(Function<long[], Boolean> pathConsumer) {
        this.pathConsumer = pathConsumer;
    }
    @Override
    public void run() {
        long nodeId;

        while (true) {
            nodeId = nextNodeSupplier.nextNode();

            if (nodeId == RandomWalk.NextNodeSupplier.NO_MORE_NODES) break;

            if (graph.degree(nodeId) == 0) {
                progressTracker.logProgress();
                continue;
            }
            var walksPerNode = config.walksPerNode();

            sampler.prepareForNewNode(nodeId);

            for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                var path= sampler.walk(nodeId);
                   boolean shouldContinue= pathConsumer.apply(path);
                    if (!shouldContinue){
                        break;
                    }
            }

            progressTracker.logProgress();
        }

    }

}
