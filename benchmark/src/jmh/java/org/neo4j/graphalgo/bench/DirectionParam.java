/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;

import java.util.function.Function;

public enum DirectionParam implements Function<GraphLoader, GraphLoader> {

    IN {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.INCOMING);
        }
    },
    IN_SORT {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.INCOMING).withSort(true);
        }
    },
    OUT {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.OUTGOING);
        }
    },
    OUT_SORT {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.OUTGOING).withSort(true);
        }
    },
    BOTH {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.BOTH);
        }
    },
    BOTH_SORT {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(Direction.BOTH).withSort(true);
        }
    },
    UNDIRECTED {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.loadAsUndirected(true).withDirection(Direction.BOTH).withSort(true);
        }
    },
    NONE {
        @Override
        public GraphLoader apply(final GraphLoader graphLoader) {
            return graphLoader.withDirection(null);
        }
    }
}
