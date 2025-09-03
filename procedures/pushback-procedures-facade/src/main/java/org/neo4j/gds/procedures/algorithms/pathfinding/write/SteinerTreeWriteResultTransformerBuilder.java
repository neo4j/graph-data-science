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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.procedures.algorithms.pathfinding.SteinerWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.stream.Stream;

class SteinerTreeWriteResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<SteinerTreeResult>, Stream<SteinerWriteResult>> {
    SteinerTreeWriteResultTransformerBuilder(SteinerTreeWriteConfig config) {}

    @Override
    public ResultTransformer<TimedAlgorithmResult<SteinerTreeResult>, Stream<SteinerWriteResult>> build(Graph graph, GraphStore graphStore) {
        return ar -> Stream.empty();
    }
}
