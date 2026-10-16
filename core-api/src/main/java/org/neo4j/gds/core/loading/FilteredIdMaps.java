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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.nodes.FilterableNodeTranslator;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.nodes.LabelInformation;
import org.neo4j.gds.api.nodes.NodeLabelConsumer;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.api.nodes.RootNodeMapping;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class FilteredIdMaps {

    private FilteredIdMaps() {}

    public static Optional<FilteredIdMap> withFilteredLabels(
        IdMap rootIdMap,
        NodeTranslator rootTranslator,
        Collection<NodeLabel> nodeLabels,
        Concurrency concurrency
    ) {
        if (!(rootTranslator instanceof FilterableNodeTranslator filterable)) {
            throw new UnsupportedOperationException(
                "This id map does not support label filtering: " + rootTranslator.typeId()
            );
        }

        var rootLabelInformation = rootIdMap.labelInformation();
        rootLabelInformation.validateNodeLabelFilter(nodeLabels);

        if (rootLabelInformation.isEmpty()) {
            return Optional.empty();
        }

        if (nodeLabels.containsAll(rootLabelInformation.availableNodeLabels())) {
            return Optional.empty();
        }

        var unionBitSet = rootLabelInformation.unionBitSet(nodeLabels);
        var filteredTranslator = filterable.filteredNodeTranslator(unionBitSet, concurrency);
        var filteredLabelInformation = rootLabelInformation.filter(
            nodeLabels,
            filteredTranslator.nodeCount(),
            filteredTranslator::toMappedNodeId
        );

        return Optional.of(new FilteredView(rootIdMap, filteredTranslator, filteredLabelInformation));
    }

    /**
     * Composes original -> root -> filtered translation; per-node label reads
     * delegate to the root id map at root mapped ids; aggregate label queries and
     * label-filtered iteration use the filtered {@link LabelInformation}.
     */
    static final class FilteredView implements FilteredIdMap {

        private final IdMap rootIdMap;
        private final NodeTranslator filteredTranslator; // root mapped id <-> filtered id
        private final LabelInformation filteredLabelInformation;
        private final RootNodeMapping rootNodeMapping;

        FilteredView(IdMap rootIdMap, NodeTranslator filteredTranslator, LabelInformation filteredLabelInformation) {
            this.rootIdMap = rootIdMap;
            this.filteredTranslator = filteredTranslator;
            this.filteredLabelInformation = filteredLabelInformation;
            this.rootNodeMapping = new RootNodeMapping() {
                @Override
                public long toRootNodeId(long filteredNodeId) {
                    return filteredTranslator.toOriginalNodeId(filteredNodeId);
                }

                @Override
                public long fromRootNodeId(long rootNodeId) {
                    return filteredTranslator.toMappedNodeId(rootNodeId);
                }
            };
        }

        @Override
        public String typeId() {return rootIdMap.typeId();}

        /**
         * If the original node id is not present in the root id map, the lookup
         * must short-circuit to {@link IdMap#NOT_FOUND}. {@code NOT_FOUND} is not
         * a valid input for the root-to-filtered lookup.
         */
        @Override
        public long toMappedNodeId(long originalNodeId) {
            var rootNodeId = rootIdMap.toMappedNodeId(originalNodeId);
            if (rootNodeId == IdMap.NOT_FOUND) {
                return IdMap.NOT_FOUND;
            }
            return filteredTranslator.toMappedNodeId(rootNodeId);
        }

        @Override
        public long toOriginalNodeId(long filteredNodeId) {
            return rootIdMap.toOriginalNodeId(filteredTranslator.toOriginalNodeId(filteredNodeId));
        }

        /**
         * If the original node id is not present in the root id map, the lookup
         * must short-circuit; see {@link #toMappedNodeId(long)}.
         */
        @Override
        public boolean containsOriginalId(long originalNodeId) {
            var rootNodeId = rootIdMap.toMappedNodeId(originalNodeId);
            if (rootNodeId == IdMap.NOT_FOUND) {
                return false;
            }
            return filteredTranslator.containsOriginalId(rootNodeId);
        }

        @Override
        public long nodeCount() {return filteredTranslator.nodeCount();}

        @Override
        public long highestOriginalId() {return rootIdMap.highestOriginalId();}

        @Override
        public long toFilteredNodeId(long rootNodeId) {return filteredTranslator.toMappedNodeId(rootNodeId);}

        @Override
        public boolean containsRootNodeId(long rootNodeId) {return filteredTranslator.containsOriginalId(rootNodeId);}

        @Override
        public IdMap rootIdMap() {return rootIdMap;}

        @Override
        public RootNodeMapping rootNodeMapping() {return rootNodeMapping;}

        @Override
        public long toRootNodeId(long filteredNodeId) {return filteredTranslator.toOriginalNodeId(filteredNodeId);}

        @Override
        public LabelInformation labelInformation() {return filteredLabelInformation;}

        @Override
        public List<NodeLabel> nodeLabels(long filteredNodeId) {
            return rootIdMap.nodeLabels(filteredTranslator.toOriginalNodeId(filteredNodeId));
        }

        @Override
        public void forEachNodeLabel(long filteredNodeId, NodeLabelConsumer consumer) {
            rootIdMap.forEachNodeLabel(filteredTranslator.toOriginalNodeId(filteredNodeId), consumer);
        }

        @Override
        public boolean hasLabel(long filteredNodeId, NodeLabel label) {
            return rootIdMap.hasLabel(filteredTranslator.toOriginalNodeId(filteredNodeId), label);
        }

        @Override
        public Set<NodeLabel> availableNodeLabels() {return filteredLabelInformation.availableNodeLabels();}

        @Override
        public long nodeCount(NodeLabel nodeLabel) {return filteredLabelInformation.nodeCountForLabel(nodeLabel);}

        @Override
        public void addNodeLabel(NodeLabel nodeLabel) {rootIdMap.addNodeLabel(nodeLabel);}

        @Override
        public void addNodeIdToLabel(long filteredNodeId, NodeLabel nodeLabel) {
            rootIdMap.addNodeIdToLabel(filteredTranslator.toOriginalNodeId(filteredNodeId), nodeLabel);
        }
    }
}
