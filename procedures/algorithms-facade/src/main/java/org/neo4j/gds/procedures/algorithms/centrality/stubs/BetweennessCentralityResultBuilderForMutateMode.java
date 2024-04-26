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
package org.neo4j.gds.procedures.algorithms.centrality.stubs;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityMutateResult;

import java.util.Map;
import java.util.Optional;

class BetweennessCentralityResultBuilderForMutateMode implements ResultBuilder<BetweennessCentralityMutateConfig, BetwennessCentralityResult, CentralityMutateResult, Pair<Map<String, Object>, NodePropertiesWritten>> {
    @Override
    public CentralityMutateResult build(
        Graph graph,
        GraphStore graphStore,
        BetweennessCentralityMutateConfig configuration,
        Optional<BetwennessCentralityResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<Map<String, Object>, NodePropertiesWritten>> metadata
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return CentralityMutateResult.emptyFrom(timings, configurationMap);

        // yeah... the presence of the result signifies that mutation happened, that the graph wasn't empty; we happen to not render anything other than the metadata
        var ignored = result.get();

        return new CentralityMutateResult(
            metadata.orElseThrow().getRight().value,
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.preProcessingMillis,
            metadata.orElseThrow().getLeft(),
            configurationMap
        );
    }
}
