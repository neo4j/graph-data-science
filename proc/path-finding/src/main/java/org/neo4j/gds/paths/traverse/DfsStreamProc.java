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

public class DfsStreamProc extends AlgoBaseProc<DFS, HugeLongArray, DfsStreamConfig, DfsStreamResult> {
    static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    static final String DESCRIPTION =
        "Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures. " +
        "The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph) " +
        "and explores as far as possible along each branch before backtracking.";

    @Procedure(name = "gds.dfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<DfsStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new DfsStreamSpec();

        return new ProcedureExecutor<>(
            streamSpec,
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(name = "gds.dfs.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new DfsStreamSpec();

        return new MemoryEstimationExecutor<>(
            streamSpec,
            executionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Override
    public GraphAlgorithmFactory<DFS, DfsStreamConfig> algorithmFactory() {
        return new DfsStreamSpec().algorithmFactory();
    }

    @Override
    protected DfsStreamConfig newConfig(String username, CypherMapWrapper config) {
        return new DfsStreamSpec().newConfigFunction().apply(username, config);
    }

    @Override
    public ComputationResultConsumer<DFS, HugeLongArray, DfsStreamConfig, Stream<DfsStreamResult>> computationResultConsumer() {
        return new DfsStreamSpec().computationResultConsumer();
    }

}
