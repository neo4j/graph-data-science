/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.neo4j.gds.embeddings.graphsage.NeighborhoodSampler;
import org.neo4j.gds.embeddings.graphsage.UniformNeighborhoodSampler;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Vector;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class WeightedMaxPoolAggregatingLayer implements WeightedLayer {

    private final UniformNeighborhoodSampler sampler;
    private final long sampleSize;
    private final RelationshipWeightsFunction relationshipWeightsFunction;
    private final Weights<Matrix> poolWeights;
    private final Weights<Matrix> selfWeights;
    private final Weights<Matrix> neighborsWeights;
    private final Weights<Vector> bias;
    private final Function<Variable<Matrix>, Variable<Matrix>> activationFunction;

    private long randomState;

    WeightedMaxPoolAggregatingLayer(
        RelationshipWeightsFunction relationshipWeightsFunction,
        long sampleSize,
        Weights<Matrix> poolWeights,
        Weights<Matrix> selfWeights,
        Weights<Matrix> neighborsWeights,
        Weights<Vector> bias,
        Function<Variable<Matrix>, Variable<Matrix>> activationFunction
    ) {
        this.relationshipWeightsFunction = relationshipWeightsFunction;
        this.poolWeights = poolWeights;
        this.selfWeights = selfWeights;
        this.neighborsWeights = neighborsWeights;
        this.bias = bias;

        this.randomState = ThreadLocalRandom.current().nextLong();

        this.sampleSize = sampleSize;
        this.sampler = new UniformNeighborhoodSampler();

        this.activationFunction = activationFunction;
    }

    @Override
    public long sampleSize() {
        return sampleSize;
    }

    @Override
    public Aggregator aggregator() {
        return new WeightedMaxPoolingAggregator(
            this.relationshipWeightsFunction,
            this.poolWeights,
            this.selfWeights,
            this.neighborsWeights,
            this.bias,
            activationFunction
        );
    }

    @Override
    public NeighborhoodSampler sampler() {
        return sampler;
    }

    @Override
    public long randomState() {
        return randomState;
    }

    @Override
    public void generateNewRandomState() {
        randomState = ThreadLocalRandom.current().nextLong();
    }
}
