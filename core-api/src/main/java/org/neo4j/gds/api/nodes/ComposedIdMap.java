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
package org.neo4j.gds.api.nodes;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.FilteredIdMaps;
import org.neo4j.gds.core.utils.LazyBatchCollection;

import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;

public final class ComposedIdMap implements IdMap {

    private final NodeTranslator nodeTranslator;
    private LabelInformation labelInformation;

    public static ComposedIdMap of(NodeTranslator nodeTranslator, LabelInformation labelInformation) {
        return new ComposedIdMap(nodeTranslator, labelInformation);

    }

    private ComposedIdMap(NodeTranslator nodeTranslator, LabelInformation labelInformation) {
        this.nodeTranslator = nodeTranslator;
        this.labelInformation = labelInformation;
    }

    public NodeTranslator nodeTranslator() {
        return this.nodeTranslator;
    }

    // NodeTranslator

    @Override
    public String typeId() {
        return this.nodeTranslator.typeId();
    }

    @Override
    public long nodeCount() {
        return this.nodeTranslator.nodeCount();
    }

    @Override
    public long highestOriginalId() {
        return this.nodeTranslator.highestOriginalId();
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return this.nodeTranslator.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return this.nodeTranslator.toMappedNodeId(originalNodeId);
    }

    @Override
    public boolean containsOriginalId(long originalNodeId) {
        return this.nodeTranslator.containsOriginalId(originalNodeId);
    }

    // PartialIdMap

    @Override
    public OptionalLong rootNodeCount() {
        return OptionalLong.of(this.nodeTranslator.nodeCount());
    }

    // NodeLabels

    @Override
    public LabelInformation labelInformation() {
        return this.labelInformation;
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        prepareForAddingNodeLabel(nodeLabel);
        this.labelInformation.addLabel(nodeLabel);
    }

    @Override
    public void addNodeIdToLabel(long mappedNodeId, NodeLabel nodeLabel) {
        prepareForAddingNodeLabel(nodeLabel);
        this.labelInformation.addNodeIdToLabel(mappedNodeId, nodeLabel);
    }

    private void prepareForAddingNodeLabel(NodeLabel nodeLabel) {
        if (this.labelInformation.isSingleLabel()) {
            this.labelInformation = labelInformation.toMultiLabel(nodeLabel);
        }
    }

    // NodeIterator / BatchNodeIterable

    @Override
    public void forEachNode(LongPredicate consumer) {
        long nodeCount = nodeCount();
        for (long i = 0; i < nodeCount; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return new IdIterator(nodeCount());
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return this.labelInformation.nodeIterator(labels, nodeCount());
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return LazyBatchCollection.of(nodeCount(), batchSize, IdIterable::new);
    }

    // IdMap root / filter

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, Concurrency concurrency) {
        return FilteredIdMaps.withFilteredLabels(this, this.nodeTranslator, nodeLabels, concurrency);

    }
}
