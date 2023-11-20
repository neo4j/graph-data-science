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
package org.neo4j.gds.projection;

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Optional;

interface ExecutingQueryProvider {
    Optional<String> executingQuery();

    static ExecutingQueryProvider fromTransaction(KernelTransactions ktxs, Transaction transaction) {
        return new TxQuery(ktxs, transaction);
    }

    static ExecutingQueryProvider empty() {
        return Optional::empty;
    }
}


final class TxQuery implements ExecutingQueryProvider {
    private final KernelTransactions ktxs;
    private final Transaction transaction;

    TxQuery(KernelTransactions ktxs, Transaction transaction) {
        this.ktxs = ktxs;
        this.transaction = transaction;
    }

    @Override
    public Optional<String> executingQuery() {
        if (!(this.transaction instanceof InternalTransaction)) {
            return Optional.empty();
        }

        var txId = Neo4jProxy.transactionId(((InternalTransaction) this.transaction).kernelTransaction());
        return this.ktxs.activeTransactions().stream()
            .filter(handle -> Neo4jProxy.transactionId(handle) == txId)
            .flatMap(handle -> handle.executingQuery().stream())
            .flatMap(eq -> eq.snapshot()
                .obfuscatedQueryText()
                .or(() -> Optional.ofNullable(eq.rawQueryText())).stream())
            .findAny();
    }
}
