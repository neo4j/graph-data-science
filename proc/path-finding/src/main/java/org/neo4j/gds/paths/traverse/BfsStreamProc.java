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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class BfsStreamProc extends AlgoBaseProc<BFS, HugeLongArray, BfsStreamConfig, BfsStreamResult> {
    static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    static final String DESCRIPTION =
        "BFS is a traversal algorithm, which explores all of the neighbor nodes at " +
        "the present depth prior to moving on to the nodes at the next depth level.";

    @Procedure(name = "gds.bfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<BfsStreamResult> bfs(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new BfsStreamSpec();

        return new ProcedureExecutor<>(
            streamSpec,
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(name = "gds.bfs.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new BfsStreamSpec();

        return new MemoryEstimationExecutor<>(
            streamSpec,
            executionContext()
        ).computeEstimate(graphName, configuration);    }

    @Override
    public GraphAlgorithmFactory<BFS, BfsStreamConfig> algorithmFactory() {
        return new BfsStreamSpec().algorithmFactory();
    }

    @Override
    protected BfsStreamConfig newConfig(String username, CypherMapWrapper config) {
        return new BfsStreamSpec().newConfigFunction().apply(username, config);
    }

    @Override
    public ComputationResultConsumer<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>> computationResultConsumer() {
        return new BfsStreamSpec().computationResultConsumer();
    }

}
