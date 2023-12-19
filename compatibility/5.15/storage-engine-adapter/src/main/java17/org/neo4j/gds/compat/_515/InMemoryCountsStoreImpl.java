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
package org.neo4j.gds.compat._515;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;

import java.io.IOException;

public class InMemoryCountsStoreImpl implements CountsStore {

    private final GraphStore graphStore;
    private final TokenHolders tokenHolders;

    public InMemoryCountsStoreImpl(
        GraphStore graphStore,
        TokenHolders tokenHolders
    ) {

        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void start(CursorContext cursorContext, MemoryTracker memoryTracker) throws IOException {

    }

    @Override
    public CountsUpdater updater(long txId, boolean isLast, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Updates are not supported");
    }

    @Override
    public void checkpoint(FileFlushEvent fileFlushEvent, CursorContext cursorContext) {

    }

    @Override
    public long nodeCount(int labelId, CursorContext cursorContext) {
        if (labelId == -1) {
            return graphStore.nodeCount();
        }

        String nodeLabel;
        try {
            nodeLabel = tokenHolders.labelTokens().getTokenById(labelId).name();
        } catch (TokenNotFoundException e) {
            throw new RuntimeException(e);
        }
        return graphStore.nodes().nodeCount(NodeLabel.of(nodeLabel));
    }

    @Override
    public long estimateNodeCount(int labelId, CursorContext cursorContext) {
        var label = tokenHolders.labelGetName(labelId);
        return graphStore.nodes().nodeCount(NodeLabel.of(label));
    }

    @Override
    public long relationshipCount(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        // TODO: this is not using start/endLabel
        if (typeId == -1) {
            return graphStore.relationshipCount();
        } else {
            var type = tokenHolders.relationshipTypeGetName(typeId);
            return graphStore.relationshipCount(RelationshipType.of(type));
        }
    }

    @Override
    public long estimateRelationshipCount(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        var type = tokenHolders.relationshipTypeGetName(typeId);
        return graphStore.relationshipCount(RelationshipType.of(type));
    }

    @Override
    public boolean consistencyCheck(
        ReporterFactory reporterFactory,
        CursorContextFactory cursorContextFactory,
        int i,
        ProgressMonitorFactory progressMonitorFactory
    ) {
        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public void accept(CountsVisitor visitor, CursorContext cursorContext) {
        tokenHolders.labelTokens().getAllTokens().forEach(labelToken -> {
            visitor.visitNodeCount(labelToken.id(), nodeCount(labelToken.id(), cursorContext));
        });

        visitor.visitRelationshipCount(-1, -1, -1, graphStore.relationshipCount());
    }
}
