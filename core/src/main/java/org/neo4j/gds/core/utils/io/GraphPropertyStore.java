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
package org.neo4j.gds.core.utils.io;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.graph.GraphProperty;

import java.util.Iterator;

class GraphPropertyStore {

    private final Iterator<GraphProperty> graphPropertyIterator;

    GraphPropertyStore(GraphStore graphStore) {
        this.graphPropertyIterator = new GraphPropertyIterator(graphStore);
    }

    Iterator<GraphProperty> graphPropertyIterator() {
        return this.graphPropertyIterator;
    }

    private static final class GraphPropertyIterator implements Iterator<GraphProperty> {

        private final GraphStore graphStore;
        private final Iterator<String> graphPropertyKeys;

        private GraphPropertyIterator(GraphStore graphStore) {
            this.graphStore = graphStore;
            this.graphPropertyKeys = graphStore.graphPropertyKeys().iterator();
        }

        @Override
        public boolean hasNext() {
            return graphPropertyKeys.hasNext();
        }

        @Override
        public GraphProperty next() {
            var graphPropertyKey = graphPropertyKeys.next();
            return graphStore.graphProperty(graphPropertyKey);
        }
    }
}
