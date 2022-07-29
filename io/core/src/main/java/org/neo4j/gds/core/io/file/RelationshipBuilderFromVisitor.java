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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

public abstract class RelationshipBuilderFromVisitor {

    public static RelationshipBuilderFromVisitor of(
        int numberOfProperties,
        RelationshipsBuilder delegate,
        RelationshipVisitor visitor
    ) {
        if (numberOfProperties == 0) {
            return new NoPropertiesBuilder(delegate, visitor);
        }
        if (numberOfProperties == 1) {
            return new SinglePropertyBuilder(delegate, visitor);
        }
        return new MultiPropertyBuilder(delegate, visitor, numberOfProperties);
    }


    final RelationshipsBuilder delegate;
    final RelationshipVisitor visitor;

    RelationshipBuilderFromVisitor(RelationshipsBuilder delegate, RelationshipVisitor visitor) {
        this.delegate = delegate;
        this.visitor = visitor;
    }

    public abstract void addFromVisitor();

    private static final class NoPropertiesBuilder extends RelationshipBuilderFromVisitor {

        NoPropertiesBuilder(RelationshipsBuilder delegate, RelationshipVisitor visitor) {
            super(delegate, visitor);
        }

        @Override
        public void addFromVisitor() {
            delegate.add(visitor.startNode(), visitor.endNode());
        }
    }

    private static final class SinglePropertyBuilder extends RelationshipBuilderFromVisitor implements PropertyConsumer {

        SinglePropertyBuilder(RelationshipsBuilder delegate, RelationshipVisitor visitor) {
            super(delegate, visitor);
        }

        @Override
        public void addFromVisitor() {
            visitor.forEachProperty(this);
        }

        @Override
        public void accept(String key, Object value) {
            delegate.add(visitor.startNode(), visitor.endNode(), Double.parseDouble(value.toString()));
        }
    }

    private static final class MultiPropertyBuilder extends RelationshipBuilderFromVisitor implements PropertyConsumer {
        private final double[] propertyValues;
        private int propertyIndex;

        MultiPropertyBuilder(
            RelationshipsBuilder delegate,
            RelationshipVisitor visitor,
            int numberOfProperties
        ) {
            super(delegate, visitor);
            propertyValues = new double[numberOfProperties];
        }

        @Override
        public void addFromVisitor() {
            propertyIndex = 0;
            visitor.forEachProperty(this);
            delegate.add(visitor.startNode(), visitor.endNode(), propertyValues);
        }

        @Override
        public void accept(String key, Object value) {
            propertyValues[propertyIndex++] = Double.parseDouble(value.toString());
        }
    }
}
