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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.gds.similarity.knn.KnnResult;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

class KnnResultBuilderForMutateMode implements ResultBuilder<KnnMutateConfig, KnnResult, KnnMutateResult, Pair<RelationshipsWritten, Map<String, Object>>> {
    /**
     * @param metadata number of relationships written, and the similarity distribution
     */
    @Override
    public KnnMutateResult build(
        Graph graph,
        GraphStore graphStore,
        KnnMutateConfig configuration,
        Optional<KnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<RelationshipsWritten, Map<String, Object>>> metadata
    ) {
        //noinspection OptionalIsPresent
        if (result.isEmpty()) return new KnnMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.postProcessingMillis,
            0,
            0,
            0,
            Collections.emptyMap(),
            false,
            0,
            0,
            configuration.toMap()
        );

        return new KnnMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.postProcessingMillis,
            0,
            result.get().nodesCompared(),
            metadata.orElseThrow().getLeft().value,
            metadata.orElseThrow().getRight(),
            result.get().didConverge(),
            result.get().ranIterations(),
            result.get().nodePairsConsidered(),
            configuration.toMap()
        );
    }
}
