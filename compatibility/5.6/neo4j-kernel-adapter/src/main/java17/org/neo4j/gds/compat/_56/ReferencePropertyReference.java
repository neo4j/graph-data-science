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
package org.neo4j.gds.compat._56;

import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.storageengine.api.Reference;

import java.util.Objects;

public final class ReferencePropertyReference implements PropertyReference {

    private static final PropertyReference EMPTY = new ReferencePropertyReference(null);

    public final Reference reference;

    private ReferencePropertyReference(Reference reference) {
        this.reference = reference;
    }

    public static PropertyReference of(Reference reference) {
        return new ReferencePropertyReference(Objects.requireNonNull(reference));
    }

    public static PropertyReference empty() {
        return EMPTY;
    }

    @Override
    public boolean isEmpty() {
        return reference == null;
    }
}
