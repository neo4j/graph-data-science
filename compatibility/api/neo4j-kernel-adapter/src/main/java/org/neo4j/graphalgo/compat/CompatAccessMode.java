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

import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;

import java.util.function.IntSupplier;

public abstract class CompatAccessMode extends RestrictedAccessMode {
    final CustomAccessMode custom;

    CompatAccessMode(CustomAccessMode custom) {
        super(Static.FULL, Static.FULL);
        this.custom = custom;
    }

    @Override
    public boolean allowsTraverseAllLabels() {
        return custom.allowsTraverseAllLabels();
    }

    @Override
    public boolean allowsTraverseNode(long... labels) {
        return custom.allowsTraverseNode(labels);
    }

    @Override
    public boolean allowsTraverseRelType(int relType) {
        return custom.allowsTraverseRelType(relType);
    }

    @Override
    public boolean allowsReadRelationshipProperty(IntSupplier relType, int propertyKey) {
        return custom.allowsReadRelationshipProperty(propertyKey);
    }
}
