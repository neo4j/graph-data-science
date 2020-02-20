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
package org.neo4j.graphalgo.graphbuilder;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs code blocks wrapped in {@link Transaction}s.
 */
public final class TestTransactionWrapper {

    private final GraphDatabaseAPI db;

    public TestTransactionWrapper(GraphDatabaseAPI db) {
        this.db = db;
    }

    public void accept(Consumer<Transaction> block) {
        try (final Transaction tx = db.beginTx()) {
            block.accept(tx);
            tx.success();
        }
    }

    public <T> T apply(Function<Transaction, T> block) {
        try (final Transaction tx = db.beginTx()) {
            T result = block.apply(tx);
            tx.success();
            return result;
        }
    }
}

