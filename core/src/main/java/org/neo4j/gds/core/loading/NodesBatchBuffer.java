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
package org.neo4j.gds.core.loading;

import org.immutables.builder.Builder;
import org.neo4j.gds.compat.PropertyReference;

import java.util.Optional;

public class NodesBatchBuffer extends RecordsBatchBuffer {

    private final boolean hasLabelInformation;

    private final NodeLabelTokenSet[] labelTokens;
    // property ids, consecutive
    private final PropertyReference[] properties;

    @Builder.Factory
    static NodesBatchBuffer nodesBatchBuffer(
        int capacity,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> readProperty
    ) {
        return new NodesBatchBuffer(
            // TODO: we probably wanna adjust the capacity here
            capacity,
            hasLabelInformation.orElse(false),
            readProperty.orElse(false)
        );
    }

    private NodesBatchBuffer(
        int capacity,
        boolean hasLabelInformation,
        boolean readProperty
    ) {
        super(capacity);
        this.hasLabelInformation = hasLabelInformation;
        this.properties = readProperty ? new PropertyReference[capacity] : null;
        this.labelTokens = new NodeLabelTokenSet[capacity];
    }

    public void add(long nodeId, PropertyReference propertyReference, NodeLabelTokenSet labelTokens) {
        int len = length++;
        buffer[len] = nodeId;
        if (properties != null) {
            properties[len] = propertyReference;
        }
        if (this.labelTokens != null) {
            this.labelTokens[len] = labelTokens;
        }
    }

    public PropertyReference[] properties() {
        return this.properties;
    }

    public boolean hasLabelInformation() {
        return hasLabelInformation;
    }

    public NodeLabelTokenSet[] labelTokens() {
        return this.labelTokens;
    }
}
