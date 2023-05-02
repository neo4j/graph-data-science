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
package org.neo4j.gds.triangle;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.triangle.TriangleProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.triangles", description = DESCRIPTION, executionMode = STREAM)
public class TriangleStreamSpecification implements AlgorithmSpec<TriangleStream, Stream<TriangleStream.Result>, TriangleCountBaseConfig, Stream<TriangleStream.Result>, TriangleStreamFactory> {

    @Override
    public String name() {
        return "TriangleStream";
    }

    @Override
    public TriangleStreamFactory algorithmFactory() {
        return new TriangleStreamFactory();
    }

    @Override
    public NewConfigFunction<TriangleCountBaseConfig> newConfigFunction() {
        return (__, userInput) -> TriangleCountBaseConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<TriangleStream, Stream<TriangleStream.Result>, TriangleCountBaseConfig, Stream<TriangleStream.Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var resultStream = computationResult.result().get();
            executionContext.closeableResourceRegistry().register(resultStream);
            return resultStream;
        };
    }
}
