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

package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.RelationshipTypes;
import org.neo4j.kernel.api.StatementConstants;

import java.util.Objects;

public abstract class RelationshipTypeMapping {

    private final String typeName;

    public static RelationshipTypeMapping of(String typeName, int typeId) {
        return new Resolved(typeName, typeId);
    }

    public static RelationshipTypeMapping all() {
        return ALL;
    }

    private RelationshipTypeMapping(String typeName) {
        this.typeName = typeName;
    }

    public final String typeName() {
        return typeName;
    }

    public abstract int typeId();

    public abstract boolean doesExist();

    private static class Resolved extends RelationshipTypeMapping {

        private final int typeId;

        Resolved(String typeName, int typeId) {
            super(typeName);
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

    private static final RelationshipTypeMapping ALL = new RelationshipTypeMapping(RelationshipTypes.ALL_IDENTIFIER) {

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
        RelationshipTypeMapping that = (RelationshipTypeMapping) o;
        return typeId() == that.typeId() &&
               typeName.equals(that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, typeId());
    }

    @Override
    public String toString() {
        return "RelationshipTypeMapping{" +
               "typeName='" + typeName + '\'' +
               ", typeId=" + typeId() +
               '}';
    }
}
