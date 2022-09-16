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

import com.carrotsearch.hppc.LongLongHashMap;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.LabeledIdMap;

import java.util.OptionalLong;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TestIdMap extends LabeledIdMap {

    private final LongLongHashMap forwardMap;
    private final LongLongHashMap reverseMap;
    private final long highestOriginalId;

    public static Builder builder() {
        return new Builder();
    }

    private TestIdMap(
        LongLongHashMap forwardMap,
        LongLongHashMap reverseMap,
        long highestOriginalId,
        LabelInformation labelInformation
    ) {
        super(labelInformation, forwardMap.size());
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
        this.highestOriginalId = highestOriginalId;
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return this.reverseMap.getOrDefault(mappedNodeId, NOT_FOUND);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return this.reverseMap.containsKey(mappedNodeId)
            ? mappedNodeId
            : NOT_FOUND;
    }

    @Override
    public boolean contains(long originalNodeId) {
        return forwardMap.containsKey(originalNodeId);
    }

    @Override
    public long highestNeoId() {
        return this.highestOriginalId;
    }

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return this.forwardMap.getOrDefault(originalNodeId, NOT_FOUND);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return OptionalLong.of(this.nodeCount());
    }

    public static final class Builder {

        private final LongLongHashMap forwardMap;
        private final LongLongHashMap reverseMap;
        private final LabelInformation.Builder labelInformationBuilder;

        private long highestOriginalId = Long.MIN_VALUE;

        public Builder() {
            this.forwardMap = new LongLongHashMap();
            this.reverseMap = new LongLongHashMap();
            this.labelInformationBuilder = LabelInformation.builder(42);
        }

        public Builder addAll(long... mappings) {
            assert mappings.length % 2 == 0 : "mapping size must be even";

            for (int i = 0; i < mappings.length; i += 2) {
                add(mappings[i], mappings[i + 1]);
            }

            return this;
        }

        public Builder add(long originalId, long internalId, NodeLabel... nodeLabels) {
            if (this.forwardMap.containsKey(originalId)) {
                throw new IllegalArgumentException(
                    formatWithLocale("original id `%d` is already mapped", originalId)
                );
            }

            if (this.reverseMap.containsKey(internalId)) {
                throw new IllegalArgumentException(
                    formatWithLocale("internal id `%d` is already mapped", internalId)
                );
            }

            if (originalId > this.highestOriginalId) {
                this.highestOriginalId = originalId;
            }

            this.forwardMap.put(originalId, internalId);
            this.reverseMap.put(internalId, originalId);

            for (NodeLabel nodeLabel : nodeLabels) {
                this.labelInformationBuilder.addNodeIdToLabel(nodeLabel, internalId);
            }

            return this;
        }

        public TestIdMap build() {
            return new TestIdMap(
                this.forwardMap,
                this.reverseMap,
                this.highestOriginalId,
                labelInformationBuilder.build(this.forwardMap.size(), operand -> operand)
            );
        }
    }
}
