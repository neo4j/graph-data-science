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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.ImmutableRelationship;
import org.neo4j.gds.core.write.Relationship;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.PredictedLink;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.stream.Stream;

public class ApproximateLinkPrediction extends LinkPrediction {
    private final KnnBaseConfig knnConfig;

    public ApproximateLinkPrediction(
        Classifier classifier,
        LinkFeatureExtractor linkFeatureExtractor,
        Graph graph,
        IdMap sourceNodeLabelIdMap,
        IdMap targetNodeLabelIdMap,
        KnnBaseConfig knnConfig,
        ProgressTracker progressTracker
    ) {
        super(
            classifier,
            linkFeatureExtractor,
            graph,
            sourceNodeLabelIdMap,
            targetNodeLabelIdMap,
            knnConfig.concurrency(),
            progressTracker
        );
        this.knnConfig = knnConfig;
    }

    public static MemoryEstimation estimate(LinkPredictionPredictPipelineBaseConfig config) {
        var knnConfig = config.approximateConfig();
        var knnEstimation = new KnnFactory<>().memoryEstimation(knnConfig);

        return MemoryEstimations.builder(ApproximateLinkPrediction.class.getSimpleName())
            .add(knnEstimation)
            .build();
    }

    @Override
    LinkPredictionResult predictLinks(LinkPredictionSimilarityComputer linkPredictionSimilarityComputer) {
        var knnResult = Knn.create(
            graph,
            knnConfig,
            linkPredictionSimilarityComputer,
            new LinkPredictionSimilarityComputer.LinkFilterFactory(graph),
            ImmutableKnnContext.of(
                Pools.DEFAULT,
                progressTracker
            )
        ).compute();

        return new Result(knnResult);
    }

    static class Result implements LinkPredictionResult {
        private final Knn.Result predictions;
        private final Map<String, Object> samplingStats;

        Result(Knn.Result knnResult) {
            this.predictions = knnResult;
            this.samplingStats = Map.of(
                "strategy", "approximate",
                "linksConsidered", knnResult.nodePairsConsidered(),
                "ranIterations", knnResult.ranIterations(),
                "didConverge", knnResult.didConverge()
            );
        }

        @Override
        public Stream<PredictedLink> stream() {
            return predictions
                .streamSimilarityResult()
                .map(i -> PredictedLink.of(i.sourceNodeId(), i.targetNodeId(), i.similarity));
        }

        @Override
        public Stream<Relationship> relationshipStream() {
            return predictions
                .streamSimilarityResult()
                .map(i -> ImmutableRelationship.of(
                    i.node1,
                    i.node2,
                    new Value[]{Values.doubleValue(i.similarity)}
                ));
        }

        @Override
        public Map<String, Object> samplingStats() {
            return samplingStats;
        }
    }
}
