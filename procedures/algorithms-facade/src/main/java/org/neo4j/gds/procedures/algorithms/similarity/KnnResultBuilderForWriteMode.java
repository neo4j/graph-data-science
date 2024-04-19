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
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class KnnResultBuilderForWriteMode implements ResultBuilder<KnnWriteConfig, KnnResult, Stream<KnnWriteResult>, Pair<RelationshipsWritten, Map<String, Object>>> {
    @Override
    public Stream<KnnWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        KnnWriteConfig configuration,
        Optional<KnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<RelationshipsWritten, Map<String, Object>>> metadata
    ) {
        if (result.isEmpty()) return Stream.of(
            new KnnWriteResult(
                timings.preProcessingMillis,
                timings.computeMillis,
                timings.postProcessingMillis,
                0,
                0,
                0,
                false,
                0,
                0,
                Collections.emptyMap(),
                configuration.toMap()
            )
        );

        KnnWriteResult knnWriteResult = new KnnWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.postProcessingMillis,
            0,
            result.get().nodesCompared(),
            metadata.orElseThrow().getLeft().value,
            result.get().didConverge(),
            result.get().ranIterations(),
            result.get().nodePairsConsidered(),
            metadata.orElseThrow().getRight(),
            configuration.toMap()
        );

        return Stream.of(knnWriteResult);
    }
}
