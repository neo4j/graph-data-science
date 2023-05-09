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

import org.neo4j.storageengine.api.LogVersionRepository;

import java.util.concurrent.atomic.AtomicLong;

public class InMemoryLogVersionRepository58 implements LogVersionRepository {

    private final AtomicLong logVersion;
    private final AtomicLong checkpointLogVersion;

    public InMemoryLogVersionRepository58() {
        this(0, 0);
    }

    private InMemoryLogVersionRepository58(long initialLogVersion, long initialCheckpointLogVersion) {
        this.logVersion = new AtomicLong();
        this.checkpointLogVersion = new AtomicLong();
        this.logVersion.set(initialLogVersion);
        this.checkpointLogVersion.set(initialCheckpointLogVersion);
    }

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

    @Override
    public long getCurrentLogVersion() {
        return this.logVersion.get();
    }

    @Override
    public long getCheckpointLogVersion() {
        return this.checkpointLogVersion.get();
    }
}
