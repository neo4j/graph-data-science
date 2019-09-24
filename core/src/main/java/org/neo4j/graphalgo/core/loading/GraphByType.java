/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.Graph;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public interface GraphByType {

    Graph loadGraph(String relationshipType, Optional<String> maybeRelationshipProperty);

    default Graph loadGraph(String relationshipType) {
        return loadGraph(relationshipType, Optional.empty());
    }

    Graph loadAllTypes();

    void release();

    String getGraphType();

    void canRelease(boolean canRelease);

    long nodeCount();

    long relationshipCount();

    Set<String> availableRelationshipTypes();

    final class SingleGraph implements GraphByType {
        private final Graph graph;

        public SingleGraph(Graph graph) {
            this.graph = graph;
        }

        @Override
        public Graph loadGraph(String relationshipType, Optional<String> maybeRelationshipProperty) {
            return graph;
        }

        @Override
        public Graph loadAllTypes() {
            return graph;
        }

        @Override
        public void canRelease(boolean canRelease) {
            graph.canRelease(canRelease);
        }

        @Override
        public void release() {
            graph.release();
        }

        @Override
        public String getGraphType() {
            return graph.getType();
        }

        @Override
        public long nodeCount() {
            return graph.nodeCount();
        }

        @Override
        public long relationshipCount() {
            return graph.relationshipCount();
        }

        @Override
        public Set<String> availableRelationshipTypes() {
            return Collections.emptySet();
        }
    }
}
