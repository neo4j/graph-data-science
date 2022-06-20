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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.internal.recordstorage.InMemoryNodeScan;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

import java.util.Arrays;

import static java.lang.Math.min;

public abstract class AbstractInMemoryNodeCursor extends NodeRecord implements StorageNodeCursor {

    private long next;
    private long highMark;

    private final GraphStore graphStore;
    private final TokenHolders tokenHolders;
    private final boolean hasProperties;
    private final long[] nodeLabelReadBuffer;
    private final MutableInt nodeLabelCounter;

    public AbstractInMemoryNodeCursor(GraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        this.hasProperties = !graphStore.nodePropertyKeys().isEmpty();
        nodeLabelReadBuffer = new long[graphStore.nodeLabels().size()];
        nodeLabelCounter = new MutableInt();
    }

    public abstract void properties(StoragePropertyCursor propertyCursor);

    @Override
    public long[] labels() {
        nodeLabelCounter.setValue(0);

        graphStore.nodes().forEachNodeLabel(getId(), nodeLabel -> {
            nodeLabelReadBuffer[nodeLabelCounter.getAndIncrement()] = tokenHolders
                .labelTokens()
                .getIdByName(nodeLabel.name());
            return true;
        });

        return Arrays.copyOf(nodeLabelReadBuffer, nodeLabelCounter.getValue());
    }

    public boolean hasAtLeastOneLabelForCurrentNode() {
        var hasLabel = new MutableBoolean(false);
        graphStore.nodes().forEachNodeLabel(getId(), __ -> {
            hasLabel.setTrue();
            return false;
        });

        return hasLabel.booleanValue();
    }

    @Override
    public boolean hasLabel(int labelId) {
        var nodeLabel = NodeLabel.of(tokenHolders.labelGetName(labelId));
        return graphStore.nodes().hasLabel(getId(), nodeLabel);
    }

    @Override
    public long relationshipsReference() {
        return getId();
    }

    @Override
    public void relationships(
        StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection
    ) {
        traversalCursor.init(getId(), -1, selection);
    }

    @Override
    public int[] relationshipTypes() {
        return new int[0];
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        return false;
    }

    @Override
    public void scan() {
        if (getId() != NO_ID) {
            resetState();
        }
        setId(0L);
        this.highMark = nodeHighMark();
    }

    @Override
    public void single(long reference) {
        if (getId() != NO_ID) {
            resetState();
        }
        this.next = reference;
        this.highMark = reference;
    }

    public boolean scanBatch(AllNodeScan scan, int sizeHint) {
        if (getId() != NO_ID) {
            reset();
        }

        return ((InMemoryNodeScan) scan).scanBatch(sizeHint, this);
    }

    public boolean scanRange(long start, long stop) {
        long max = nodeHighMark();
        if (start > max) {
            reset();
            return false;
        }
        if (start > stop) {
            reset();
            return true;
        }
        next = start;
        this.highMark = min(stop, max);
        return true;
    }

    @Override
    public boolean hasProperties() {
        return this.hasProperties;
    }

    @Override
    public long entityReference() {
        return getId();
    }

    @Override
    public boolean next() {
        if (next == NO_ID) {
            resetState();
            return false;
        }

        setId(next);
        node(this, getId());
        next++;

        if (next > this.highMark) {
            next = NO_ID;
        }
        return true;
    }

    @Override
    public void reset() {
        resetState();
    }

    private void resetState() {
        setId(NO_ID);
        next = NO_ID;
        this.highMark = NO_ID;
        clear();
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }

    private long nodeHighMark() {
        return graphStore.nodeCount() - 1;
    }

    private void node(NodeRecord record, long nodeId) {
        record.setId(nodeId);
    }
}
