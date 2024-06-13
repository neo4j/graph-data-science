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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringWriteConfig;

import java.util.Optional;
import java.util.stream.Stream;

class K1ColoringResultBuilderForWriteMode implements ResultBuilder<K1ColoringWriteConfig, K1ColoringResult, Stream<K1ColoringWriteResult>, Void> {
    private final boolean computeUsedColors;

    K1ColoringResultBuilderForWriteMode(boolean computeUsedColors) {this.computeUsedColors = computeUsedColors;}

    @Override
    public Stream<K1ColoringWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        K1ColoringWriteConfig configuration,
        Optional<K1ColoringResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> metadata
    ) {
        if (result.isEmpty()) return Stream.of(K1ColoringWriteResult.emptyFrom(timings, configuration.toMap()));

        var k1ColoringResult = result.get();

        long usedColors = (computeUsedColors) ? k1ColoringResult.usedColors().cardinality() : 0;

        var k1ColoringWriteResult = new K1ColoringWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            k1ColoringResult.colors().size(),
            usedColors,
            k1ColoringResult.ranIterations(),
            k1ColoringResult.didConverge(),
            configuration.toMap()
        );

        return Stream.of(k1ColoringWriteResult);
    }
}
