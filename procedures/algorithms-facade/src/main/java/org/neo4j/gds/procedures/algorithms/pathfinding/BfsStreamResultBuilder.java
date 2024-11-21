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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.graphdb.RelationshipType;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.algorithms.pathfinding.BfsStreamResult.RELATIONSHIP_TYPE_NAME;

class BfsStreamResultBuilder implements StreamResultBuilder<HugeLongArray, BfsStreamResult> {
    private final NodeLookup nodeLookup;
    private final boolean pathRequested;
    private final BfsStreamConfig configuration;

    BfsStreamResultBuilder(NodeLookup nodeLookup, boolean pathRequested, BfsStreamConfig configuration) {
        this.nodeLookup = nodeLookup;
        this.pathRequested = pathRequested;
        this.configuration = configuration;
    }

    @Override
    public Stream<BfsStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<HugeLongArray> result
    ) {
        //noinspection OptionalIsPresent
        if (result.isEmpty()) return Stream.empty();

        return TraverseStreamComputationResultConsumer.consume(
            configuration.sourceNode(),
            result.get(),
            graph::toOriginalNodeId,
            BfsStreamResult::new,
            PathFactoryFacade.create(pathRequested, nodeLookup,graphStore),
            RelationshipType.withName(RELATIONSHIP_TYPE_NAME),
            nodeLookup
        );
    }
}
