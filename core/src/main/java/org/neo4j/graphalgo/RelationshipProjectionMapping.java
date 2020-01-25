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
import org.neo4j.kernel.api.StatementConstants;

@ValueClass
public interface RelationshipProjectionMapping {

    RelationshipProjectionMapping ALL = ImmutableRelationshipProjectionMapping.builder()
        .typeName("")
        .elementIdentifier("")
        .projection(Projection.NATURAL)
        .typeId(StatementConstants.ANY_RELATIONSHIP_TYPE)
        .doesExist(true)
        .build();

    String elementIdentifier();

    String typeName();

    Projection projection();

    int typeId();

    boolean doesExist();

    static RelationshipProjectionMapping of(String typeName, int typeId) {
        return of(typeName, typeName, Projection.NATURAL, typeId);
    }

    static RelationshipProjectionMapping of(
        String elementIdentifier,
        String typeName,
        Projection projection,
        int typeId
    ) {
        return ImmutableRelationshipProjectionMapping.builder()
            .elementIdentifier(elementIdentifier)
            .typeName(typeName)
            .projection(projection)
            .typeId(typeId)
            .doesExist(typeId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE)
            .build();
    }
}
