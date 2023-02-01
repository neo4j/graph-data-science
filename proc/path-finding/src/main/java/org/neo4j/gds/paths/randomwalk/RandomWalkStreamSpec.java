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
package org.neo4j.gds.paths.randomwalk;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkAlgorithmFactory;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;


@GdsCallable(name = "gds.randomWalk.stream", description = RandomWalkStreamProc.DESCRIPTION, executionMode = STREAM)
public class RandomWalkStreamSpec implements AlgorithmSpec<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, Stream<StreamResult>, RandomWalkAlgorithmFactory<RandomWalkStreamConfig>> {
    @Override
    public String name() {
        return "RandomWalkStream";
    }

    @Override
    public RandomWalkAlgorithmFactory<RandomWalkStreamConfig> algorithmFactory() {
        return new RandomWalkAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<RandomWalkStreamConfig> newConfigFunction() {
        return (__, config) -> RandomWalkStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            if (graph.isEmpty()) {
                return Stream.empty();
            }

            var returnPath = executionContext.callContext()
                .outputFields()
                .anyMatch(field -> toLowerCaseWithLocale(field).equals("path"));

            Function<List<Long>, Path> pathCreator = returnPath
                ? (List<Long> nodes) -> PathFactory.create(
                executionContext.nodeLookup(),
                nodes,
                RelationshipType.withName("NEXT")
            )
                : (List<Long> nodes) -> null;

            return Optional.ofNullable(computationResult.result()).orElseGet(Stream::empty)
                .map(nodes -> {
                    var translatedNodes = translateInternalToNeoIds(nodes, graph);
                    var path = pathCreator.apply(translatedNodes);

                    return new StreamResult(translatedNodes, path);
                });
        };
    }

    private List<Long> translateInternalToNeoIds(long[] nodes, IdMap idMap) {
        var translatedNodes = new ArrayList<Long>(nodes.length);
        for (int i = 0; i < nodes.length; i++) {
            translatedNodes.add(i, idMap.toOriginalNodeId(nodes[i]));
        }
        return translatedNodes;

    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
