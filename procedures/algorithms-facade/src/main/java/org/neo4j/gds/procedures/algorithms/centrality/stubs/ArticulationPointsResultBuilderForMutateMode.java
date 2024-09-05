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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointsMutateResult;

import java.util.Optional;

public class ArticulationPointsResultBuilderForMutateMode implements ResultBuilder<ArticulationPointsMutateConfig, BitSet, ArticulationPointsMutateResult, NodePropertiesWritten> {

    @Override
    public ArticulationPointsMutateResult build(
        Graph graph,
        ArticulationPointsMutateConfig configuration,
        Optional<BitSet> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return ArticulationPointsMutateResult.EMPTY;


        var bitSet = result.get();
        return new ArticulationPointsMutateResult(
                bitSet.cardinality(),
                metadata.map(NodePropertiesWritten::value).orElseThrow(),
                timings.mutateOrWriteMillis,
                timings.computeMillis,
                configuration.toMap()
        );
    }
}
