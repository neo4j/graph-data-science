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
package org.neo4j.gds.compat;

import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;

/**
 * The signature for {@code allowsReadNodeProperty} is different in 4.0 and 4.1.
 * We have to implement this method, but we don't require the parameter that has changed.
 * All methods that we re-implement on {@code AccessMode} have been duplicated in {@code CustomAccessMode}.
 * Usages where we need to implement the {@code AccessMode} should implement {@code CustomAccessMode} instead and
 * then call the {@link Neo4jProxyApi#accessMode(CustomAccessMode)} method to get the actual access mode.
 */
public abstract class CompatAccessMode extends RestrictedAccessMode {
    protected final CustomAccessMode custom;

    protected CompatAccessMode(CustomAccessMode custom) {
        super(Static.FULL, Static.FULL);
        this.custom = custom;
    }

    @Override
    public boolean allowsTraverseAllLabels() {
        return custom.allowsTraverseAllLabels();
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel(long label) {
        return custom.allowTraverseAllNodesWithLabel(label);
    }

    @Override
    public boolean allowsTraverseNode(long... labels) {
        return custom.allowsTraverseNode(labels);
    }

    @Override
    public boolean allowsTraverseRelType(int relType) {
        return custom.allowsTraverseRelType(relType);
    }
}
