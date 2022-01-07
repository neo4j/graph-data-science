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
package org.neo4j.gds.shortestpath;

import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.ShortestPathDeltaStepping;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.shortestpath.ShortestPathDeltaSteppingProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

/**
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 */
@GdsCallable(name = "gds.alpha.shortestPath.deltaStepping.stream", description = DESCRIPTION, executionMode = STREAM)
public class ShortestPathDeltaSteppingStreamProc extends ShortestPathDeltaSteppingProc<ShortestPathDeltaStepping.DeltaSteppingResult> {

    @Procedure(name = "gds.alpha.shortestPath.deltaStepping.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<ShortestPathDeltaStepping, ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig, Stream<ShortestPathDeltaStepping.DeltaSteppingResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.graph().isEmpty()) {
                return Stream.empty();
            }

            return computationResult.result().resultStream();
        };
    }
}
