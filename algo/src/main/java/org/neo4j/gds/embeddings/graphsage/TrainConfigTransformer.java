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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainMemoryEstimateParameters;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainParameters;

public final class TrainConfigTransformer {

    private TrainConfigTransformer() {}

    public static GraphSageTrainMemoryEstimateParameters toMemoryEstimateParameters(GraphSageTrainConfig config) {
        int estimationFeatureDimension = config.projectedFeatureDimension()
            .orElse(config.featureProperties().size());
        var layerConfigs = GraphSageHelper.layerConfigs(
            estimationFeatureDimension,
            config.sampleSizes(),
            config.randomSeed(),
            config.aggregator(),
            config.activationFunction(),
            config.embeddingDimension()
        );
        return new GraphSageTrainMemoryEstimateParameters(
            layerConfigs,
            config.isMultiLabel(),
            config.featureProperties().size(),
            estimationFeatureDimension,
            config.batchSize(),
            config.embeddingDimension()
        );
    }

    public static GraphSageTrainParameters toParameters(GraphSageTrainConfig config) {
        return new GraphSageTrainParameters(
            config.concurrency(),
            config.batchSize(),
            config.maxIterations(),
            config.searchDepth(),
            config.epochs(),
            config.learningRate(),
            config.tolerance(),
            config.negativeSampleWeight(),
            config.penaltyL2(),
            config.embeddingDimension(),
            config.sampleSizes(),
            config.featureProperties(),
            config.maybeBatchSamplingRatio(),
            config.randomSeed(),
            config.aggregator(),
            config.activationFunction()
        );
    }
}
