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
package org.neo4j.graphalgo;

import org.neo4j.kernel.api.StatementConstants;

import java.util.Objects;

public abstract class RelationshipProjectionMapping {

    private final String elementIdentifier;

    private final String typeName;

    private final Projection projection;

    public static RelationshipProjectionMapping of(String typeName, int typeId) {
        return new Resolved(typeName, typeName, null, typeId);
    }

    public static RelationshipProjectionMapping of(String elementIdentifier, String typeName, Projection projection, int typeId) {
        return new Resolved(elementIdentifier, typeName, projection, typeId);
    }

    public static RelationshipProjectionMapping all() {
        return ALL;
    }

    private RelationshipProjectionMapping(String elementIdentifier, String typeName, Projection projection) {
        this.elementIdentifier = elementIdentifier;
        this.typeName = typeName;
        this.projection = projection;
    }

    public final String elementIdentifier() {
        return elementIdentifier;
    }

    public final String typeName() {
        return typeName;
    }

    public final Projection projection() {
        return projection;
    }

    public abstract int typeId();

    public abstract boolean doesExist();

    private static class Resolved extends RelationshipProjectionMapping {

        private final int typeId;

        Resolved(String elementIdentifier, String typeName, Projection projection, int typeId) {
            super(elementIdentifier, typeName, projection);
            this.typeId = typeId;
        }

        @Override
        public int typeId() {
            return typeId;
        }

        @Override
        public boolean doesExist() {
            return typeId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
        }
    }

    private static final RelationshipProjectionMapping ALL = new RelationshipProjectionMapping("", "", Projection.NATURAL) {

        @Override
        public int typeId() {
            return StatementConstants.ANY_RELATIONSHIP_TYPE;
        }

        @Override
        public boolean doesExist() {
            return true;
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipProjectionMapping that = (RelationshipProjectionMapping) o;
        return typeId() == that.typeId() &&
               typeName.equals(that.typeName) &&
               elementIdentifier.equals(that.elementIdentifier) &&
               projection == that.projection;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, typeId(), elementIdentifier, projection);
    }

    @Override
    public String toString() {
        return "RelationshipProjectionMapping{" +
               "elementIdentifier='" + elementIdentifier + '\'' +
               ", typeName='" + typeName + '\'' +
               ", projection='" + projection + '\'' +
               ", typeId=" + typeId() +
               '}';
    }
}
