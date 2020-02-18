/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.compat;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs code blocks wrapped in {@link KernelTransaction}s.
 * <p>
 * Calls to this class may already run in a {@link Transaction}, in which case the wrapper uses a
 * {@code PlaceboTransaction} to not close the top-level transaction prematurely.
 * <p>
 * Implementation Note: We're not obtaining the KernelTransaction in a try-with-resources statement because
 * we don't want to call {@link KernelTransaction#close()} on it. We leave closing to the surrounding {@link Transaction}.
 * If it was indeed a {@code TopLevelTransaction} &ndash; and not a {@code PlaceboTransaction} &ndash; we will close
 * the {@link KernelTransaction} as well, otherwise we'll leave it open.
 */
public final class TransactionWrapper {
    private final GraphDatabaseAPI db;

    public TransactionWrapper(GraphDatabaseAPI db) {
        this.db = db;
    }

    public void accept(Consumer<KernelTransaction> block) {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
            block.accept(transaction);
            tx.commit();
        }
    }

    public <T> T apply(Function<KernelTransaction, T> block) {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
            T result = block.apply(transaction);
            tx.commit();
            return result;
        }
    }
}
