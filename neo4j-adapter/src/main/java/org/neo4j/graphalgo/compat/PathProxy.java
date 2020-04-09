/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.compat;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.Objects;

public abstract class PathProxy implements Path {

    /**
     * TOMBSTONE that the user has to return from the API
     * and the only way to get there is by calling a method on
     * PropertyConsumer.
     * Albeit returning null, this forces the user to return either
     * a node or a relationship, not both or none, implementing a
     * poor man's approach to sum types.
     */
    public static final class PropertyResult {
        private static final PropertyResult INSTANCE = new PropertyResult();

        private PropertyResult() {
        }
    }

    public interface PropertyConsumer {
        PropertyResult returnNode(Node node);

        PropertyResult returnRelationship(Relationship relationship);
    }

    public interface PropertyIterator {
        boolean hasNext();

        PropertyResult next(PropertyConsumer consumer);
    }

    private static final class PropertyHolder implements PropertyConsumer {
        private Node node;
        private Relationship relationship;

        private PropertyHolder() {
        }

        @Override
        public PropertyResult returnNode(Node node) {
            this.node = Objects.requireNonNull(node);
            this.relationship = null;
            return PropertyResult.INSTANCE;
        }

        @Override
        public PropertyResult returnRelationship(Relationship relationship) {
            this.node = null;
            this.relationship = Objects.requireNonNull(relationship);
            return PropertyResult.INSTANCE;
        }

        private Entity get() {
            try {
                if (node != null) {
                    return node;
                } else if (relationship != null) {
                    return relationship;
                } else {
                    throw new IllegalStateException("No value has been set");
                }
            } finally {
                this.node = null;
                this.relationship = null;
            }
        }
    }

    public abstract PropertyIterator toIterator();

    @Override
    public final Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private final PropertyHolder holder = new PropertyHolder();
            private final PropertyIterator iter = toIterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entity next() {
                Objects.requireNonNull(
                    iter.next(holder),
                    "A value must be returned by calling one of the return* methods"
                );
                return holder.get();
            }
        };
    }
}
