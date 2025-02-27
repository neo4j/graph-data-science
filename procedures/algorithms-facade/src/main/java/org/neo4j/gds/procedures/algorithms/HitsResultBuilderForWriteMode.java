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
package org.neo4j.gds.procedures.algorithms;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.procedures.algorithms.centrality.HitsWriteResult;

import java.util.Optional;
import java.util.stream.Stream;

class HitsResultBuilderForWriteMode implements ResultBuilder<HitsConfig,PregelResult, Stream<HitsWriteResult>, NodePropertiesWritten> {

    @Override
    public Stream<HitsWriteResult> build(
        Graph graph,
        HitsConfig configuration,
        Optional<PregelResult> pregelResult,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        return Stream.of(
            pregelResult
                .map( result-> new HitsWriteResult(result.ranIterations(),
                    result.didConverge(),
                    timings.preProcessingMillis,
                    timings.computeMillis,
                    timings.sideEffectMillis,
                    metadata.map(NodePropertiesWritten::value).orElseThrow(),
                    configuration.toMap()))
                .orElse(HitsWriteResult.emptyFrom(timings,configuration.toMap())));
    }
}



