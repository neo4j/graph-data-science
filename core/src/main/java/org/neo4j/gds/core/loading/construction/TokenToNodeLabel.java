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
import org.neo4j.gds.NodeLabel;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class TokenToNodeLabel {

    final ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping;
    final IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping;

    static TokenToNodeLabel fixed(
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping
    ) {
        return new FixedTokenToNodeLabel(elementIdentifierLabelTokenMapping, labelTokenNodeLabelMapping);
    }

    static TokenToNodeLabel lazy() {
        return new LazyTokenToNodeLabel();
    }

    TokenToNodeLabel(
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping
    ) {
        this.elementIdentifierLabelTokenMapping = elementIdentifierLabelTokenMapping;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
    }

    IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping() {
        return this.labelTokenNodeLabelMapping;
    }

    abstract int getOrCreateToken(NodeLabel nodeLabel);

    private static class FixedTokenToNodeLabel extends TokenToNodeLabel {

        FixedTokenToNodeLabel(
            ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
            IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping
        ) {
            super(elementIdentifierLabelTokenMapping, labelTokenNodeLabelMapping);
        }

        @Override
        public int getOrCreateToken(NodeLabel nodeLabel) {
            if (!elementIdentifierLabelTokenMapping.containsKey(nodeLabel)) {
                throw new IllegalArgumentException(formatWithLocale("No token was specified for node label %s", nodeLabel));
            }
            return elementIdentifierLabelTokenMapping.get(nodeLabel);
        }
    }

    private static class LazyTokenToNodeLabel extends TokenToNodeLabel {

        private final Lock lock;
        private int nextLabelId;

        LazyTokenToNodeLabel() {
            super(new ObjectIntScatterMap<>(), new IntObjectHashMap<>());
            this.lock = new ReentrantLock(true);
            this.nextLabelId = 0;
        }

        @Override
        public int getOrCreateToken(NodeLabel nodeLabel) {
            var token = elementIdentifierLabelTokenMapping.getOrDefault(nodeLabel, NO_SUCH_LABEL);
            if (token == NO_SUCH_LABEL) {
                lock.lock();
                token = elementIdentifierLabelTokenMapping.getOrDefault(nodeLabel, NO_SUCH_LABEL);
                if (token == NO_SUCH_LABEL) {
                    token = nextLabelId++;
                    labelTokenNodeLabelMapping.put(token, Collections.singletonList(nodeLabel));
                    elementIdentifierLabelTokenMapping.put(nodeLabel, token);
                }
                lock.unlock();
            }
            return token;
        }
    }
}
