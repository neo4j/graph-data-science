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

import org.neo4j.graphalgo.annotation.ValueClass;

import static org.neo4j.graphalgo.compat.StatementConstantsProxy.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_RELATIONSHIP_TYPE;

@ValueClass
public interface RelationshipProjectionMapping {

    static RelationshipProjectionMapping all() {
        return all(Orientation.NATURAL);
    }

    static RelationshipProjectionMapping all(Orientation orientation) {
        return ImmutableRelationshipProjectionMapping.builder()
            .typeName("")
            .elementIdentifier("")
            .orientation(orientation)
            .typeId(ANY_RELATIONSHIP_TYPE)
            .exists(true)
            .build();
    }

    String elementIdentifier();

    String typeName();

    Orientation orientation();

    int typeId();

    boolean exists();

    static RelationshipProjectionMapping of(String typeName, int typeId) {
        return of(typeName, typeName, Orientation.NATURAL, typeId);
    }

    static RelationshipProjectionMapping of(
        String elementIdentifier,
        String typeName,
        Orientation orientation,
        int typeId
    ) {
        return ImmutableRelationshipProjectionMapping.builder()
            .elementIdentifier(elementIdentifier)
            .typeName(typeName)
            .orientation(orientation)
            .typeId(typeId)
            .exists(typeId != NO_SUCH_RELATIONSHIP_TYPE)
            .build();
    }
}
