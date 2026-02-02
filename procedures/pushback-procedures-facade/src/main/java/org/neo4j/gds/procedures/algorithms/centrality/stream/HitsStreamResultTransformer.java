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
package org.neo4j.gds.procedures.algorithms.centrality.stream;


import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.hits.HitsResultWithGraph;
import org.neo4j.gds.procedures.algorithms.centrality.HitsStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class HitsStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<HitsResultWithGraph>, Stream<HitsStreamResult>> {

    private String authProperty;
    private String hubProperty;
    private IdMap idMap;

    public HitsStreamResultTransformer(String authProperty, String hubProperty, IdMap idMap) {
        this.authProperty = authProperty;
        this.hubProperty = hubProperty;
        this.idMap = idMap;
    }

    @Override
    public Stream<HitsStreamResult> apply(TimedAlgorithmResult<HitsResultWithGraph> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result().pregelResult();
        var auth = result.nodeValues().doubleProperties(authProperty);
        var hub = result.nodeValues().doubleProperties(hubProperty);

        return LongStream.range(IdMap.START_NODE_ID, auth.size())
            .mapToObj(nodeId -> {
                // for every schema element
                var authValue = auth.get(nodeId);
                var hubValue = hub.get(nodeId);
                return new HitsStreamResult(
                        idMap.toOriginalNodeId(nodeId), Map.of(
                        authProperty,authValue,
                        hubProperty,hubValue
                    )
                );
            });
    }
}
