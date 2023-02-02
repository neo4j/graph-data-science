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
package org.neo4j.gds.core.loading.construction;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntScatterMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class TokenToNodeLabels {

    final ObjectIntMap<NodeLabel> nodeLabelToLabelTokenMap;
    final IntObjectHashMap<List<NodeLabel>> labelTokenToNodeLabelMap;

    static TokenToNodeLabels fixed(Collection<NodeLabel> nodeLabels) {
        var elementIdentifierLabelTokenMapping = new ObjectIntScatterMap<NodeLabel>();
        var labelTokenNodeLabelMapping = new IntObjectHashMap<List<NodeLabel>>();
        var labelTokenCounter = new MutableInt(0);
        nodeLabels.forEach(nodeLabel -> {
            int labelToken = nodeLabel == NodeLabel.ALL_NODES
                ? ANY_LABEL
                : labelTokenCounter.getAndIncrement();

            elementIdentifierLabelTokenMapping.put(nodeLabel, labelToken);
            labelTokenNodeLabelMapping.put(labelToken, List.of(nodeLabel));
        });

        return new Fixed(elementIdentifierLabelTokenMapping, labelTokenNodeLabelMapping);
    }

    static TokenToNodeLabels lazy() {
        return new Lazy();
    }

    private TokenToNodeLabels() {
        this.nodeLabelToLabelTokenMap = new ObjectIntScatterMap<>();
        this.labelTokenToNodeLabelMap = new IntObjectHashMap<>();
    }

    private TokenToNodeLabels(
        ObjectIntMap<NodeLabel> nodeLabelToLabelTokenMap,
        IntObjectHashMap<List<NodeLabel>> labelTokenToNodeLabelMap
    ) {
        this.nodeLabelToLabelTokenMap = nodeLabelToLabelTokenMap;
        this.labelTokenToNodeLabelMap = labelTokenToNodeLabelMap;
    }

    IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping() {
        return this.labelTokenToNodeLabelMap;
    }

    abstract int getOrCreateToken(NodeLabel nodeLabel);

    private static final class Fixed extends TokenToNodeLabels {

        private Fixed(
            ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
            IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping
        ) {
            super(elementIdentifierLabelTokenMapping, labelTokenNodeLabelMapping);
        }

        @Override
        public int getOrCreateToken(NodeLabel nodeLabel) {
            if (!nodeLabelToLabelTokenMap.containsKey(nodeLabel)) {
                throw new IllegalArgumentException(formatWithLocale("No token was specified for node label %s", nodeLabel));
            }
            return nodeLabelToLabelTokenMap.get(nodeLabel);
        }
    }

    private static final class Lazy extends TokenToNodeLabels {

        private int nextLabelId;

        private Lazy() {
            this.nextLabelId = 0;
        }

        @Override
        public int getOrCreateToken(NodeLabel nodeLabel) {
            var token = nodeLabelToLabelTokenMap.getOrDefault(nodeLabel, NO_SUCH_LABEL);
            if (token == NO_SUCH_LABEL) {
                token = nextLabelId++;
                labelTokenToNodeLabelMap.put(token, Collections.singletonList(nodeLabel));
                nodeLabelToLabelTokenMap.put(nodeLabel, token);
            }
            return token;
        }
    }
}
