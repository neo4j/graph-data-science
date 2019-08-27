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

package org.neo4j.graphalgo.core;

import java.util.Optional;

public enum Direction {

    OUTGOING {
        @Override
        public Optional<org.neo4j.graphdb.Direction> toNeo() {
            return Optional.of(org.neo4j.graphdb.Direction.OUTGOING);
        }
    },
    INCOMING {
        @Override
        public Optional<org.neo4j.graphdb.Direction> toNeo() {
            return Optional.of(org.neo4j.graphdb.Direction.INCOMING);
        }
    },
    BOTH {
        @Override
        public Optional<org.neo4j.graphdb.Direction> toNeo() {
            return Optional.of(org.neo4j.graphdb.Direction.BOTH);
        }
    },
    UNDIRECTED {
        @Override
        public Optional<org.neo4j.graphdb.Direction> toNeo() {
            return Optional.empty();
        }
    };

    public abstract Optional<org.neo4j.graphdb.Direction> toNeo();

    public static Direction fromNeo(org.neo4j.graphdb.Direction neoDirection) {
        switch (neoDirection) {
            case OUTGOING:
                return Direction.OUTGOING;
            case INCOMING:
                return Direction.INCOMING;
            case BOTH:
                return Direction.BOTH;
            default:
                throw new IllegalArgumentException("Unexpected direction " + neoDirection);
        }
    }
}
