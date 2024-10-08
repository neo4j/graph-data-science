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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.triangle.TriangleResult;

import java.util.Optional;
import java.util.stream.Stream;

class TrianglesResultBuilderForStreamMode implements StreamResultBuilder<Stream<TriangleResult>, TriangleStreamResult> {
    private final CloseableResourceRegistry closeableResourceRegistry;

    TrianglesResultBuilderForStreamMode(CloseableResourceRegistry closeableResourceRegistry) {
        this.closeableResourceRegistry = closeableResourceRegistry;
    }

    @Override
    public Stream<TriangleStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<Stream<TriangleResult>> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var triangles = result.get().map(t -> new TriangleStreamResult(t.nodeA, t.nodeB, t.nodeC));

        closeableResourceRegistry.register(triangles);

        return triangles;
    }
}
