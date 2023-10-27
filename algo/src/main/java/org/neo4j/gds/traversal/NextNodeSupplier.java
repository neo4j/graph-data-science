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
package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@FunctionalInterface
public interface NextNodeSupplier {
    long NO_MORE_NODES = -1;

    long nextNode();

    class GraphNodeSupplier implements NextNodeSupplier {
        private final long numberOfNodes;
        private final AtomicLong nextNodeId;

        GraphNodeSupplier(long numberOfNodes) {
            this.numberOfNodes = numberOfNodes;
            this.nextNodeId = new AtomicLong(0);
        }

        @Override
        public long nextNode() {
            var nextNode = nextNodeId.getAndIncrement();
            return nextNode < numberOfNodes ? nextNode : NO_MORE_NODES;
        }
    }

    final class ListNodeSupplier implements NextNodeSupplier {
        private final List<Long> nodes;
        private final AtomicInteger nextIndex;

        static ListNodeSupplier of(List<Long> sourceNodes, Graph graph) {
            var mappedIds = sourceNodes.stream().map(graph::toMappedNodeId).collect(Collectors.toList());
            return new ListNodeSupplier(mappedIds);
        }

        private ListNodeSupplier(List<Long> nodes) {
            this.nodes = nodes;
            this.nextIndex = new AtomicInteger(0);
        }

        @Override
        public long nextNode() {
            var index = nextIndex.getAndIncrement();
            return index < nodes.size() ? nodes.get(index) : NO_MORE_NODES;
        }
    }
}
