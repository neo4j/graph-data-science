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
package org.neo4j.gds.beta.randomwalk;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkAlgorithmFactory;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.randomwalk.RandomWalkStreamProc.DESCRIPTION;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.randomWalk.stream", description = DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class RandomWalkStreamProc extends AlgoBaseProc<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, RandomWalkStreamProc.RandomWalkResult> {

    static final String DESCRIPTION =
        "Random Walk is an algorithm that provides random paths in a graph. " +
        "It’s similar to how a drunk person traverses a city.";

    @Procedure(name = "gds.beta.randomWalk.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<RandomWalkResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration, false, false);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(value = "gds.beta.randomWalk.stream.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected RandomWalkStreamConfig newConfig(String username, CypherMapWrapper config) {
        return RandomWalkStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<RandomWalk, RandomWalkStreamConfig> algorithmFactory() {
        return new RandomWalkAlgorithmFactory<>();
    }

    @Override
    public ComputationResultConsumer<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, Stream<RandomWalkResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.graph().isEmpty()) {
                computationResult.graph().release();
                return Stream.empty();
            }

            var returnPath = callContext
                .outputFields()
                .anyMatch(field -> toLowerCaseWithLocale(field).equals("path"));

            Function<List<Long>, Path> pathCreator = returnPath
                ? (List<Long> nodes) -> PathFactory.create(procedureTransaction, nodes, RelationshipType.withName("NEXT"))
                : (List<Long> nodes) -> null;

            return computationResult.result()
                .map(nodes -> {
                    var translatedNodes = translateInternalToNeoIds(nodes, computationResult.graph());
                    var path = pathCreator.apply(translatedNodes);

                    return new RandomWalkResult(translatedNodes, path);
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

    public static final class RandomWalkResult {
        public List<Long> nodeIds;
        public Path path;

        RandomWalkResult(List<Long> nodeIds, Path path) {
            this.nodeIds = nodeIds;
            this.path = path;
        }
    }

}
