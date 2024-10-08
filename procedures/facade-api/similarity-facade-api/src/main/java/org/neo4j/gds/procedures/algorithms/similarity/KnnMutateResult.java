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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;

import java.util.Collections;
import java.util.Map;

public class KnnMutateResult extends SimilarityMutateResult {
    public final long ranIterations;
    public final long nodePairsConsidered;
    public final boolean didConverge;

    public KnnMutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long postProcessingMillis,
        long nodesCompared,
        long relationshipsWritten,
        Map<String, Object> similarityDistribution,
        boolean didConverge,
        long ranIterations,
        long nodePairsConsidered,
        Map<String, Object> configuration
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            mutateMillis,
            postProcessingMillis,
            nodesCompared,
            relationshipsWritten,
            similarityDistribution,
            configuration
        );

        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.nodePairsConsidered = nodePairsConsidered;
    }

    static KnnMutateResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new KnnMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            0,
            0,
            0,
            Collections.emptyMap(),
            false,
            0,
            0,
            configurationMap
        );
    }

    static KnnMutateResult create(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap,
        RelationshipsWritten relationshipsWritten,
        Map<String, Object> similarityDistribution,
        long nodesCompared,
        boolean didConverge,
        long ranIterations,
        long nodePairsConsidered
    ) {
        return new KnnMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            0,
            nodesCompared,
            relationshipsWritten.value(),
            similarityDistribution,
            didConverge,
            ranIterations,
            nodePairsConsidered,
            configurationMap
        );
    }
}
