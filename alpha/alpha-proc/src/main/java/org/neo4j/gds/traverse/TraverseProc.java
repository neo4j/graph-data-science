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
package org.neo4j.gds.traverse;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.impl.traverse.Traverse;
import org.neo4j.gds.impl.traverse.TraverseConfig;
import org.neo4j.gds.impl.walking.WalkResult;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TraverseProc extends AlgoBaseProc<Traverse, Traverse, TraverseConfig, WalkResult> {
    private static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    @Override
    public GraphAlgorithmFactory<Traverse, TraverseConfig> algorithmFactory() {
        return new TraverseFactory<>();
    }

    @Override
    public ComputationResultConsumer<Traverse, Traverse, TraverseConfig, Stream<WalkResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.graph().isEmpty()) {
                return Stream.empty();
            }

            Traverse traverse = computationResult.algorithm();
            long[] nodes = traverse.resultNodes();
            var nodeList = Arrays.stream(nodes).boxed().collect(Collectors.toList());
            return Stream.of(new WalkResult(nodes,
                PathFactory.create(transaction.internalTransaction(), nodeList, NEXT)
            ));
        };
    }
}
