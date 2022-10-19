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
package org.neo4j.gds.compat._51;

import org.neo4j.gds.compat.CompatAccessMode;
import org.neo4j.gds.compat.CustomAccessMode;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;

import java.util.function.Supplier;

public final class CompatAccessModeImpl extends CompatAccessMode {

    CompatAccessModeImpl(CustomAccessMode custom) {
        super(custom);
    }

    @Override
    public boolean allowsReadNodeProperty(Supplier<TokenSet> labels, int propertyKey) {
        return custom.allowsReadNodeProperty(propertyKey);
    }

    @Override
    public boolean allowsReadRelationshipProperty(RelTypeSupplier relType, int propertyKey) {
        return custom.allowsReadRelationshipProperty(propertyKey);
    }
}
