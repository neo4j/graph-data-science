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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;
import org.neo4j.gds.similarity.knn.ImmutableKnnBaseConfig;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;

import java.util.Collection;
import java.util.Optional;

public class ApproximateLinkPrediction extends LinkPrediction {
    private final KnnBaseConfig knnConfig;

    public ApproximateLinkPrediction(
        LinkLogisticRegressionData modelData,
        PipelineExecutor pipelineExecutor,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        GraphStore graphStore,
        int concurrency,
        Optional<Long> randomSeed,
        int topK,
        double deltaThreshold,
        int maxIterations,
        int randomJoins,
        double sampleRate,
        ProgressTracker progressTracker
    ) {
        super(modelData, pipelineExecutor, nodeLabels, relationshipTypes, graphStore, concurrency, progressTracker);
        knnConfig = ImmutableKnnBaseConfig
            .builder()
            .concurrency(concurrency)
            .randomSeed(randomSeed)
            .topK(topK)
            .randomJoins(randomJoins)
            .deltaThreshold(deltaThreshold)
            .maxIterations(maxIterations)
            .sampleRate(sampleRate)
            .build();
    }

    @Override
    LinkPredictionResult predictLinks(
        Graph graph,
        LinkFeatureExtractor featureExtractor,
        LinkLogisticRegressionPredictor predictor
    ) {
        var linkPredictionSimilarityComputer = new LinkPredictionSimilarityComputer(
            featureExtractor,
            predictor,
            graph
        );

        var randomSamplingSimilarityResult = new Knn(
            graph.nodeCount(),
            knnConfig,
            linkPredictionSimilarityComputer,
            ImmutableKnnContext.of(
                Pools.DEFAULT,
                AllocationTracker.empty(),
                progressTracker
            )
        ).compute();

//        randomSamplingSimilarityResult
//            .streamSimilarityResult()
//            .forEach(similarityResult -> {
//                result.add(
//                    similarityResult.node1,
//                    similarityResult.node2,
//                    similarityResult.similarity
//                );
//            });
        // TODO fix this
        return new LinkPredictionResult(1);
    }
}
