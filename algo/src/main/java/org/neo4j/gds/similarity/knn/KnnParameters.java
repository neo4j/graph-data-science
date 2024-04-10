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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.annotation.Parameters;

import java.util.List;
import java.util.Optional;

@Parameters
public record KnnParameters(
    int concurrency,
    int maxIterations,
    double similarityCutoff,
    K kHolder,
    double perturbationRate,
    int randomJoins,
    int minBatchSize,
    KnnSampler.SamplerType samplerType,
    Optional<Long> randomSeed,
    List<KnnNodePropertySpec> nodePropertySpecs) {

    static KnnParameters create(
        long nodeCount,
        int concurrency,
        int maxIterations,
        double similarityCutoff,
        double deltaThreshold,
        double sampleRate,
        int rawK,
        double perturbationRate,
        int randomJoins,
        int minBatchSize,
        KnnSampler.SamplerType samplerType,
        Optional<Long> randomSeed,
        List<KnnNodePropertySpec> nodePropertySpecs
    ) {
        // concurrency -- no test atm, it probably shouldn't be here
        // maxIterations -- must be 1 or more
        if (maxIterations < 1) throw new IllegalArgumentException("maxIterations");
        // similarityCutoff -- value range [0.0;1.0]
        if (Double.compare(similarityCutoff, 0.0) < 0 || Double.compare(similarityCutoff, 1.0) > 0)
            throw new IllegalArgumentException(
                "similarityCutoff must be more than or equal to 0.0 and less than or equal to 1.0");
        // sampleRate -- value range (0.0;1.0]
        if (Double.compare(sampleRate, 0.0) < 1 || Double.compare(sampleRate, 1.0) > 0)
            throw new IllegalArgumentException("sampleRate must be more than 0.0 and less than or equal to 1.0");
        // deltaThreshold -- value range [0.0;1.0]
        if (Double.compare(deltaThreshold, 0.0) < 0 || Double.compare(deltaThreshold, 1.0) > 0)
            throw new IllegalArgumentException(
                "deltaThreshold must be more than or equal to 0.0 and less than or equal to 1.0");
        // rawK -- user provided k value must be at least 1
        if (rawK < 1) throw new IllegalArgumentException("K k must be 1 or more");
        // perturbationRate -- value range [0.0;1.0]
        if (Double.compare(perturbationRate, 0.0) < 0 || Double.compare(perturbationRate, 1.0) > 0)
            throw new IllegalArgumentException(
                "perturbationRate must be more than or equal to 0.0 and less than or equal to 1.0");
        // randomJoins -- 0 or more
        if (randomJoins < 0) throw new IllegalArgumentException("randomJoins must be 0 or more");

        return new KnnParameters(
            concurrency,
            maxIterations,
            similarityCutoff,
            K.create(rawK, nodeCount, sampleRate, deltaThreshold),
            perturbationRate,
            randomJoins,
            minBatchSize,
            samplerType,
            randomSeed,
            nodePropertySpecs
        );
    }
}
