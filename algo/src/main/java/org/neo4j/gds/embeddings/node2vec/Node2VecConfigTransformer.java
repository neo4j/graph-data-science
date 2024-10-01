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
package org.neo4j.gds.embeddings.node2vec;

public final class Node2VecConfigTransformer {
    private Node2VecConfigTransformer() {}

    public static Node2VecParameters node2VecParameters(Node2VecBaseConfig config) {
        var walkParameters = config.walkParameters();

        var samplingWalkParameters = new SamplingWalkParameters(
            walkParameters.walksPerNode(),
            walkParameters.walkLength(),
            walkParameters.returnFactor(),
            walkParameters.inOutFactor(),
            config.positiveSamplingFactor(),
            config.negativeSamplingExponent()
        );
        var trainParameters = new TrainParameters(
            config.initialLearningRate(),
            config.minLearningRate(),
            config.iterations(),
            config.windowSize(),
            config.negativeSamplingRate(),
            config.embeddingDimension(),
            config.embeddingInitializer()
        );

        return new Node2VecParameters(samplingWalkParameters, trainParameters);
    }

}
