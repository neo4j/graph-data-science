/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

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
    private final ThreadToStatementContextBridge bridge;

    public TransactionWrapper(
            final GraphDatabaseAPI db) {
        this(db, db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class));
    }

    public TransactionWrapper(
            final GraphDatabaseAPI db,
            final ThreadToStatementContextBridge bridge) {
        this.db = db;
        this.bridge = bridge;
    }

    public void accept(Consumer<KernelTransaction> block) {
        try (final Transaction tx = db.beginTx()) {
            final KernelTransaction transaction = bridge.getKernelTransactionBoundToThisThread(true);
            block.accept(transaction);
            tx.success();
        }
    }

    public <T> T apply(Function<KernelTransaction, T> block) {
        try (final Transaction tx = db.beginTx()) {
            final KernelTransaction transaction = bridge.getKernelTransactionBoundToThisThread(true);
            T result = block.apply(transaction);
            tx.success();
            return result;
        }
    }

    public int applyAsInt(ToIntFunction<KernelTransaction> block) {
        try (final Transaction tx = db.beginTx()) {
            final KernelTransaction transaction = bridge.getKernelTransactionBoundToThisThread(true);
            int result = block.applyAsInt(transaction);
            tx.success();
            return result;
        }
    }

    public double applyAsDouble(ToDoubleFunction<KernelTransaction> block) {
        try (final Transaction tx = db.beginTx()) {
            final KernelTransaction transaction = bridge.getKernelTransactionBoundToThisThread(true);
            double result = block.applyAsDouble(transaction);
            tx.success();
            return result;
        }
    }
}
