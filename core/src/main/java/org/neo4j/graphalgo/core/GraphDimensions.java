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
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.RelationshipTypeMappings;
import org.neo4j.graphalgo.api.GraphSetup;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class GraphDimensions {

    private long nodeCount;
    private final long highestNeoId;
    private long maxRelCount;
    private final int labelId;
    private final int relWeightId;
    private final RelationshipTypeMappings relTypeMappings;
    private final KernelPropertyMapping[] nodeProperties;

    public GraphDimensions(
            long nodeCount,
            long highestNeoId,
            long maxRelCount,
            int labelId,
            int relWeightId,
            RelationshipTypeMappings relTypeMappings,
            KernelPropertyMapping[] nodeProperties) {
        this.nodeCount = nodeCount;
        this.highestNeoId = highestNeoId;
        this.maxRelCount = maxRelCount;
        this.labelId = labelId;
        this.relWeightId = relWeightId;
        this.relTypeMappings = relTypeMappings;
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

    public RelationshipTypeMappings relationshipTypeMappings() {
        return relTypeMappings;
    }

    public int relWeightId() {
        return relWeightId;
    }

    public Iterable<KernelPropertyMapping> nodeProperties() {
        return Arrays.asList(nodeProperties);
    }

    public void checkValidNodePredicate(GraphSetup setup) {
        if (nonEmpty(setup.startLabel) && labelId() == NO_SUCH_LABEL) {
            throw new IllegalArgumentException(String.format("Node label not found: '%s'", setup.startLabel));
        }
    }

    public void checkValidRelationshipTypePredicate(GraphSetup setup) {
        if (nonEmpty(setup.relationshipType)) {
            String missingTypes = relTypeMappings
                    .stream()
                    .filter(m -> !m.doesExist())
                    .map(RelationshipTypeMapping::typeName)
                    .collect(joining("', '"));
            if (!missingTypes.isEmpty()) {
                throw new IllegalArgumentException(String.format(
                        "Relationship type(s) not found: '%s'",
                        missingTypes));
            }
        }
    }

    public void checkValidNodeProperties() {
        for (KernelPropertyMapping nodeProperty : nodeProperties) {
            int id = nodeProperty.propertyKeyId;
            String propertyKey = nodeProperty.propertyKeyNameInGraph;
            if (nonEmpty(propertyKey) && id == NO_SUCH_PROPERTY_KEY) {
                throw new IllegalArgumentException(String.format(
                        "Node property not found: '%s'",
                        propertyKey));
            }
        }
    }

    public void checkValidRelationshipProperty(GraphSetup setup) {
        if (nonEmpty(setup.relationWeightPropertyName) && relWeightId == NO_SUCH_PROPERTY_KEY) {
            throw new IllegalArgumentException(String.format(
                    "Relationship property not found: '%s'",
                    setup.relationWeightPropertyName));
        }
    }

    private static boolean nonEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    public static class Builder {
        private long nodeCount;
        private long highestNeoId;
        private long maxRelCount;
        private int labelId;
        private int relWeightId;
        private RelationshipTypeMappings relationshipTypeMappings;
        private KernelPropertyMapping[] nodeProperties;

        public Builder setNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setHighestNeoId(long highestNeoId) {
            this.highestNeoId = highestNeoId;
            return this;
        }

        public Builder setMaxRelCount(long maxRelCount) {
            this.maxRelCount = maxRelCount;
            return this;
        }

        public Builder setLabelId(int labelId) {
            this.labelId = labelId;
            return this;
        }

        public Builder setRelationshipTypeMappings(RelationshipTypeMappings relationshipTypeMappings) {
            this.relationshipTypeMappings = relationshipTypeMappings;
            return this;
        }

        public Builder setRelWeightId(int relWeightId) {
            this.relWeightId = relWeightId;
            return this;
        }

        public Builder setNodeProperties(KernelPropertyMapping[] nodeProperties) {
            this.nodeProperties = nodeProperties;
            return this;
        }

        public GraphDimensions build() {
            return new GraphDimensions(
                    nodeCount,
                    highestNeoId,
                    maxRelCount,
                    labelId,
                    relWeightId,
                    relationshipTypeMappings,
                    nodeProperties);
        }
    }
}
