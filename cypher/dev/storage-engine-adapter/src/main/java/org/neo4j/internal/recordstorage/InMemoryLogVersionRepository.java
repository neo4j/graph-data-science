/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.neo4j.internal.recordstorage;

public class InMemoryLogVersionRepository extends AbstractInMemoryLogVersionRepository {

    @Override
    public void setCurrentLogVersion(long version) {
        this.logVersion.set(version);
    }

    @Override
    public long incrementAndGetVersion() {
        return this.logVersion.incrementAndGet();
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        this.checkpointLogVersion.set(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return this.checkpointLogVersion.incrementAndGet();
    }
}
