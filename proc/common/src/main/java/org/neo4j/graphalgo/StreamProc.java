/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.AlgoBaseConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public abstract class StreamProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends AlgoBaseConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract PROC_RESULT streamResult(long nodeId, long originalNodeId, ALGO_RESULT computationResult);

    protected Stream<PROC_RESULT> stream(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }
        Graph graph = computationResult.graph();
        return LongStream
            .range(IdMapping.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> streamResult(nodeId, graph.toOriginalNodeId(nodeId), computationResult.result()));
    }
}
