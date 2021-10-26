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
package org.neo4j.gds.walking;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkAlgorithmFactory;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class RandomWalkProc extends AlgoBaseProc<RandomWalk, Stream<long[]>, RandomWalkStreamConfig> {

    private static final String DESCRIPTION =
        "Random Walk is an algorithm that provides random paths in a graph. " +
        "It’s similar to how a drunk person traverses a city.";

    @Procedure(name = "gds.alpha.randomWalk.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<RandomWalkResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphNameOrConfig, configuration, false, false);

        if (computationResult.graph().isEmpty()) {
            computationResult.graph().release();
            return Stream.empty();
        }

        Function<long[], Path> pathCreator = computationResult.config().returnPath()
            ? (long[] nodes) ->  PathFactory.create(procedureTransaction, nodes, RelationshipType.withName("NEXT"))
            : (long[] nodes) -> null;

        return computationResult.result()
            .map(nodes -> {
                translateInternalToNeoIds(nodes, computationResult.graph());
                return new RandomWalkResult(nodes, pathCreator.apply(nodes));
            });
    }

    @Override
    protected RandomWalkStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return RandomWalkStreamConfig.of(graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<RandomWalk, RandomWalkStreamConfig> algorithmFactory() {
        return new RandomWalkAlgorithmFactory<>();
    }

    private void translateInternalToNeoIds(long[] nodes, IdMapping nodeMapping) {
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeMapping.toOriginalNodeId(nodes[i]);
        }
    }

    public static final class RandomWalkResult {
        public long[] nodes;
        public Path path;

        RandomWalkResult(long[] nodes, Path path) {
            this.nodes = nodes;
            this.path = path;
        }
    }

}
