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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.annotation.Parameters;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.embeddings.graphsage.ActivationFunctionType;
import org.neo4j.gds.embeddings.graphsage.AggregatorType;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

@Parameters
public record GraphSageTrainParameters(
    Concurrency concurrency,
    int batchSize,
    int maxIterations,
    int searchDepth,
    int epochs,
    double learningRate,
    double tolerance,
    int negativeSampleWeight,
    double penaltyL2,
    int embeddingDimension,
    List<Integer> sampleSizes,
    List<String> featureProperties,
    Optional<Double> maybeBatchSamplingRatio,
    Optional<Long> randomSeed,
    AggregatorType aggregatorType,
    ActivationFunctionType activationFunction
) {

    public long numberOfBatches(long nodeCount) {
        return (long) Math.ceil(nodeCount / (double) batchSize);
    }
    public int batchesPerIteration(long nodeCount) {
        var samplingRatio = maybeBatchSamplingRatio().orElse(Math.min(1.0, batchSize * concurrency.value() / (double) nodeCount));
        return (int) Math.ceil(samplingRatio * numberOfBatches(nodeCount));
    }

    public List<LayerConfig> layerConfigs(int featureDimension) {
        Random random = new Random();
        randomSeed.ifPresent(random::setSeed);

        List<LayerConfig> result = new ArrayList<>(sampleSizes.size());
        for (int i = 0; i < sampleSizes.size(); i++) {
            LayerConfig layerConfig = LayerConfig.builder()
                .aggregatorType(aggregatorType)
                .activationFunction(activationFunction)
                .rows(embeddingDimension)
                .cols(i == 0 ? featureDimension : embeddingDimension)
                .sampleSize(sampleSizes.get(i))
                .randomSeed(random.nextLong())
                .build();
            result.add(layerConfig);
        }
        return result;
    }
}
