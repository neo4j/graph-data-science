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
package org.neo4j.gds.paths;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.stream.Stream;

public abstract class ShortestPathStreamProc<
    ALGO extends Algorithm<DijkstraResult>,
    CONFIG extends AlgoBaseConfig> extends StreamProc<ALGO, DijkstraResult, StreamResult, CONFIG> {

    @Override
    protected StreamResult streamResult(long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues) {
        throw new UnsupportedOperationException("Shortest path algorithm handles result building individually.");
    }

    @Override
    public ComputationResultConsumer<ALGO, DijkstraResult, CONFIG, Stream<StreamResult>> computationResultConsumer() {
        return new ShortestPathStreamResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
