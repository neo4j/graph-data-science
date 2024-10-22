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
package org.neo4j.gds.wcc;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.wcc.Constants.WCC_DESCRIPTION;

@GdsCallable(name = "gds.wcc.stream", description = WCC_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class WccStreamSpecification implements AlgorithmSpec<Wcc, DisjointSetStruct, WccStreamConfig, Stream<WccStreamSpecification.StreamResult>, WccAlgorithmFactory<WccStreamConfig>> {

    @Override
    public String name() {
        return "WccStream";
    }

    @Override
    public WccAlgorithmFactory<WccStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccStreamConfig> newConfigFunction() {
        return (__, userInput) -> WccStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;

        public final long componentId;

        public StreamResult(long nodeId, long componentId) {
            this.nodeId = nodeId;
            this.componentId = componentId;
        }
    }
}
