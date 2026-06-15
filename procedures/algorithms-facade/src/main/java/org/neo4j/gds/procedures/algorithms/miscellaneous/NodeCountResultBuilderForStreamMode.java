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
package org.neo4j.gds.procedures.algorithms.miscellaneous;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.nodecount.NodeCountResult;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Turns the algorithm result into the rows the {@code gds.nodeCount.stream} procedure yields.
 * The count is a single scalar, so we emit exactly one row (or none on an empty graph result).
 */
class NodeCountResultBuilderForStreamMode implements StreamResultBuilder<NodeCountResult, NodeCountStreamResult> {

    @Override
    public Stream<NodeCountStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<NodeCountResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        return Stream.of(new NodeCountStreamResult(result.get().nodeCount()));
    }
}
