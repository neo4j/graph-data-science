package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.FeaturePipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.LongStream;

public class LinkPrediction extends Algorithm<LinkPrediction, LinkPredictionResult> {

    private final LinkLogisticRegressionData modelData;
    private final FeaturePipeline featurePipeline;
    private final String graphName;
    private final List<NodeLabel> nodeLabels;
    private final List<RelationshipType> relationshipTypes;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final int topN;
    private final double threshold;

    public LinkPrediction(
        LinkLogisticRegressionData modelData,
        FeaturePipeline featurePipeline,
        String graphName,
        List<NodeLabel> nodeLabels,
        List<RelationshipType> relationshipTypes,
        Graph graph,
        int batchSize,
        int concurrency,
        int topN,
        double threshold,
        ProgressTracker progressTracker
    ) {
        this.modelData = modelData;
        this.featurePipeline = featurePipeline;
        this.graphName = graphName;
        this.nodeLabels = nodeLabels;
        this.relationshipTypes = relationshipTypes;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.topN = topN;
        this.threshold = threshold;
        this.progressTracker = progressTracker;
    }

    @Override
    public LinkPredictionResult compute() {
        computeNodeProperties();
        return predictLinks();
    }

    private void computeNodeProperties() {
        progressTracker.beginSubTask();
        featurePipeline.executeNodePropertySteps(graphName, nodeLabels, relationshipTypes);
        progressTracker.endSubTask();
    }

    private LinkPredictionResult predictLinks() {
        progressTracker.beginSubTask();
        var predictor = new LinkLogisticRegressionPredictor(modelData);
        var result = new LinkPredictionResult(topN);
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize);
        batchQueue.parallelConsume(concurrency, ignore -> new LinkPredictionScoreByIdsConsumer(
            graph.concurrentCopy(),
            featurePipeline.linkFeatureExtractor(graph),
            predictor,
            result,
            progressTracker
        ));
        progressTracker.endSubTask();
        return result;
    }

    @Override
    public LinkPrediction me() {
        return this;
    }

    @Override
    public void release() {

    }

    private final class LinkPredictionScoreByIdsConsumer implements Consumer<Batch> {
        private final Graph graph;
        private final LinkFeatureExtractor linkFeatureExtractor;
        private final LinkLogisticRegressionPredictor predictor;
        private final LinkPredictionResult predictedLinks;
        private final ProgressTracker progressTracker;

        private LinkPredictionScoreByIdsConsumer(
            Graph graph,
            LinkFeatureExtractor linkFeatureExtractor,
            LinkLogisticRegressionPredictor predictor,
            LinkPredictionResult predictedLinks,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.linkFeatureExtractor = linkFeatureExtractor;
            this.predictor = predictor;
            this.predictedLinks = predictedLinks;
            this.progressTracker = progressTracker;
        }

        @Override
        public void accept(Batch batch) {
            for (long sourceId : batch.nodeIds()) {
                var largerNeighbors = largerNeighbors(sourceId);
                // since graph is undirected, only process pairs where sourceId < targetId
                var smallestTarget = sourceId + 1;
                LongStream.range(smallestTarget, graph.nodeCount()).forEach(targetId -> {
                        if (largerNeighbors.contains(targetId)) return;
                        var features = linkFeatureExtractor.extractFeatures(sourceId, targetId);
                        var probability = predictor.predictedProbability(features);
                        if (probability < threshold) return;
                        predictedLinks.add(sourceId, targetId, probability);
                    }
                );
            }
            progressTracker.logProgress(batch.size());
        }

        private LongHashSet largerNeighbors(long sourceId) {
            var neighbors = new LongHashSet();
            graph.forEachRelationship(
                sourceId, (src, trg) -> {
                    if (src < trg) neighbors.add(trg);
                    return true;
                }
            );
            return neighbors;
        }
    }
}
