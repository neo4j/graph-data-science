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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.procedures.algorithms.community.SpeakerListenerLPAStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.sllpa.SpeakerListenerLPA.LABELS_PROPERTY;

public final class SpeakerListenerLPAStreamTransformer implements ResultTransformer<TimedAlgorithmResult<PregelResult>, Stream<SpeakerListenerLPAStreamResult>> {

    private final Graph graph;

    public SpeakerListenerLPAStreamTransformer(Graph graph) {this.graph = graph;}

    @Override
    public Stream<SpeakerListenerLPAStreamResult> apply(
        TimedAlgorithmResult<PregelResult> timedAlgorithmResult
    ) {
        var pregelResult = timedAlgorithmResult.result();
        if (pregelResult.ranIterations() == 0) return Stream.empty();

        var labels = pregelResult.nodeValues().longArrayProperties(LABELS_PROPERTY);

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> {
                // for every schema element
                var values = labels.get(nodeId);
                return new SpeakerListenerLPAStreamResult(graph.toOriginalNodeId(nodeId), Map.of(LABELS_PROPERTY, values));
            });
    }
}
