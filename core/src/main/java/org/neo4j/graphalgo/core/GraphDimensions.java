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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.KernelPropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.StatementConstants;

import java.util.Arrays;

import static org.neo4j.internal.kernel.api.Read.ANY_LABEL;

public final class GraphDimensions {

    private long nodeCount;
    private final long highestNeoId;
    private long maxRelCount;
    private final int labelId;
    private final int[] relationId;
    private final int relWeightId;
    private final KernelPropertyMapping[] nodeProperties;

    public GraphDimensions(
            final long nodeCount,
            final long highestNeoId,
            final long maxRelCount,
            final int labelId,
            final int[] relationId,
            final int relWeightId,
            final KernelPropertyMapping[] nodeProperties) {
        this.nodeCount = nodeCount;
        this.highestNeoId = highestNeoId;
        this.maxRelCount = maxRelCount;
        this.labelId = labelId;
        this.relationId = relationId;
        this.relWeightId = relWeightId;
        this.nodeProperties = nodeProperties;
    }

    public long nodeCount() {
        return nodeCount;
    }

    public void nodeCount(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    public long highestNeoId() {
        return highestNeoId;
    }

    public int nodeCountAsInt() {
        return Math.toIntExact(nodeCount);
    }

    public long maxRelCount() {
        return maxRelCount;
    }

    public void maxRelCount(long maxRelCount) {
        this.maxRelCount = maxRelCount;
    }

    public int labelId() {
        return labelId;
    }

    public int[] relationshipTypeId() {
        return relationId;
    }

    public int singleRelationshipTypeId() {
        return relationId == null ? Read.ANY_RELATIONSHIP_TYPE : relationId[0];
    }

    public int relWeightId() {
        return relWeightId;
    }

    public Iterable<KernelPropertyMapping> nodeProperties() {
        return Arrays.asList(nodeProperties);
    }

    public void checkValidNodePredicate(GraphSetup setup) {
        if (!(setup.startLabel == null || setup.startLabel.isEmpty()) && labelId() == ANY_LABEL) {
            throw new IllegalArgumentException(String.format("Node label not found: '%s'", setup.startLabel));
        }
    }

    public void checkValidRelationshipTypePredicate(GraphSetup setup) {
        if (!(setup.relationshipType == null || setup.relationshipType.isEmpty()) && singleRelationshipTypeId() == ANY_LABEL) {
            throw new IllegalArgumentException(String.format(
                    "Relationship type not found: '%s'",
                    setup.relationshipType));
        }
    }

    public void checkValidNodeProperties() {
        for (KernelPropertyMapping nodeProperty : nodeProperties) {
            int id = nodeProperty.propertyKeyId;
            String propertyKey = nodeProperty.propertyKeyNameInGraph;
            if (!(propertyKey == null || propertyKey.isEmpty()) && id == StatementConstants.NO_SUCH_PROPERTY_KEY) {
                throw new IllegalArgumentException(String.format(
                        "Node property not found: '%s'",
                        propertyKey));
            }
        }
    }

    public void checkValidRelationshipProperty(GraphSetup setup) {
        if (!(setup.relationWeightPropertyName == null || setup.relationWeightPropertyName.isEmpty()) && relWeightId == StatementConstants.NO_SUCH_PROPERTY_KEY) {
            throw new IllegalArgumentException(String.format(
                    "Relationship property not found: '%s'",
                    setup.relationWeightPropertyName));
        }
    }

    public static class Builder {
        private long nodeCount;
        private long highestNeoId;
        private long maxRelCount;
        private int labelId;
        private int[] relationId;
        private int relWeightId;
        private KernelPropertyMapping[] nodeProperties;

        public Builder setNodeCount(final long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setHighestNeoId(final long highestNeoId) {
            this.highestNeoId = highestNeoId;
            return this;
        }

        public Builder setMaxRelCount(final long maxRelCount) {
            this.maxRelCount = maxRelCount;
            return this;
        }

        public Builder setLabelId(final int labelId) {
            this.labelId = labelId;
            return this;
        }

        public Builder setRelationId(final int[] relationId) {
            this.relationId = relationId;
            return this;
        }

        public Builder setRelWeightId(final int relWeightId) {
            this.relWeightId = relWeightId;
            return this;
        }

        public Builder setNodeProperties(final KernelPropertyMapping[] nodeProperties) {
            this.nodeProperties = nodeProperties;
            return this;
        }

        public GraphDimensions build() {
            return new GraphDimensions(
                    nodeCount,
                    highestNeoId,
                    maxRelCount,
                    labelId,
                    relationId,
                    relWeightId,
                    nodeProperties);
        }
    }
}
