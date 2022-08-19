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
package org.neo4j.internal.recordstorage;

import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryRelationshipCursor;
import org.neo4j.internal.recordstorage.AbstractInMemoryStorageReader.AbstractAllRelationshipScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.token.TokenHolders;

import static java.lang.Math.min;

public abstract class AbstractInMemoryRelationshipScanCursor extends InMemoryRelationshipCursor implements StorageRelationshipScanCursor {

    private long highMark;

    public AbstractInMemoryRelationshipScanCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public void scan() {
        reset();
        this.sourceId = 0;
        this.selection = RelationshipSelection.ALL_RELATIONSHIPS;
        this.highMark = maxRelationshipId;
    }

    @Override
    public void single(long reference) {
        reset();
        setId(reference - 1);
        this.highMark = reference;
        this.selection = RelationshipSelection.ALL_RELATIONSHIPS;

        initializeForRelationshipReference(reference);
    }

    @Override
    public boolean next() {
        if (super.next()) {
            return getId() <= highMark;
        } else {
            this.sourceId++;
            if (this.sourceId >= graphStore.nodeCount()) {
                return false;
            } else {
                resetCursors();
                return next();
            }
        }
    }

    public boolean scanBatch(AllRelationshipsScan scan, int sizeHint) {
        if (getId() != NO_ID) {
            reset();
        }

        highMark = maxRelationshipId;
        return ((AbstractAllRelationshipScan) scan).scanBatch(sizeHint, this);
    }

    public boolean scanRange(long start, long stop) {
        reset();
        this.selection = RelationshipSelection.ALL_RELATIONSHIPS;
        highMark = min(stop, maxRelationshipId);

        if (start > maxRelationshipId) {
            return false;
        }
        initializeForRelationshipReference(start);
        return true;
    }
}
