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

import java.lang.reflect.Array;
import java.util.Optional;

public final class NodesBatchBuffer<PROPERTY_REF> extends RecordsBatchBuffer {

    private final boolean hasLabelInformation;
    private final NodeLabelTokenSet[] labelTokens;
    private final PROPERTY_REF[] propertyReferences;

    @Builder.Factory
    static <PROPERTY_REF> NodesBatchBuffer<PROPERTY_REF> nodesBatchBuffer(
        int capacity,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> readProperty,
        Class<PROPERTY_REF> propertyReferenceClass
    ) {
        return new NodesBatchBuffer<>(
            // TODO: we probably wanna adjust the capacity here
            capacity,
            hasLabelInformation.orElse(false),
            readProperty.orElse(false),
            propertyReferenceClass
        );
    }

    private NodesBatchBuffer(
        int capacity,
        boolean hasLabelInformation,
        boolean readProperty,
        Class<PROPERTY_REF> propertyReferenceClass
    ) {
        super(capacity);
        this.hasLabelInformation = hasLabelInformation;
        this.labelTokens = new NodeLabelTokenSet[capacity];
        //noinspection unchecked
        this.propertyReferences = readProperty ? (PROPERTY_REF[]) Array.newInstance(propertyReferenceClass, capacity) : null;
    }

    public void add(long nodeId, PROPERTY_REF propertyReference, NodeLabelTokenSet labelTokens) {
        int len = length++;
        buffer[len] = nodeId;
        if (propertyReferences != null) {
            propertyReferences[len] = propertyReference;
        }
        if (this.labelTokens != null) {
            this.labelTokens[len] = labelTokens;
        }
    }

    public PROPERTY_REF[] propertyReferences() {
        return this.propertyReferences;
    }

    public boolean hasLabelInformation() {
        return hasLabelInformation;
    }

    public NodeLabelTokenSet[] labelTokens() {
        return this.labelTokens;
    }
}
