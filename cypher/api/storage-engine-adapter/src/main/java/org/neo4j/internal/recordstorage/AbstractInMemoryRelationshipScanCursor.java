/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
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

        initializeForRelationshipReference(start);
        return true;
    }
}
