/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.neo4j.internal.recordstorage;

import org.neo4j.io.pagecache.context.CursorContext;

public class InMemoryLogVersionRepository extends AbstractInMemoryLogVersionRepository {

    @Override
    public void setCurrentLogVersion(long version, CursorContext cursorContext) {
        this.logVersion.set(version);
    }

    @Override
    public long incrementAndGetVersion(CursorContext cursorContext) {
        return this.logVersion.incrementAndGet();
    }

    @Override
    public void setCheckpointLogVersion(long version, CursorContext cursorContext) {
        this.checkpointLogVersion.set(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion(CursorContext cursorContext) {
        return this.checkpointLogVersion.incrementAndGet();
    }
}
