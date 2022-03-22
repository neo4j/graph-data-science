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

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractInMemoryMetaDataProvider implements MetadataProvider {
    private final ExternalStoreId externalStoreId;
    protected final AbstractInMemoryLogVersionRepository logVersionRepository;

    protected AbstractInMemoryMetaDataProvider(AbstractInMemoryLogVersionRepository logVersionRepository) {
        this.logVersionRepository = logVersionRepository;
        this.externalStoreId = new ExternalStoreId(UUID.randomUUID());
    }

    public abstract AbstractTransactionIdStore transactionIdStore();

    AbstractInMemoryLogVersionRepository logVersionRepository() {
        return logVersionRepository;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getCurrentLogVersion() {
        return this.logVersionRepository.getCurrentLogVersion();
    }

    @Override
    public long getCheckpointLogVersion() {
        return this.logVersionRepository.getCheckpointLogVersion();
    }

    @Override
    public StoreId getStoreId() {
        return StoreId.UNKNOWN;
    }

    @Override
    public Optional<ExternalStoreId> getExternalStoreId() {
        return Optional.of(this.externalStoreId);
    }

    @Override
    public long nextCommittingTransactionId() {
        return this.transactionIdStore().nextCommittingTransactionId();
    }

    @Override
    public long committingTransactionId() {
        return this.transactionIdStore().committingTransactionId();
    }

    @Override
    public long getLastCommittedTransactionId() {
        return this.transactionIdStore().getLastCommittedTransactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return this.transactionIdStore().getLastCommittedTransaction();
    }

    @Override
    public long getLastClosedTransactionId() {
        return this.transactionIdStore().getLastClosedTransactionId();
    }

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.LATEST;
    }

    @Override
    public Optional<UUID> getDatabaseIdUuid(CursorContext cursorTracer) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void setDatabaseIdUuid(UUID uuid, CursorContext cursorContext) {
        throw new IllegalStateException("Not supported");
    }
}
