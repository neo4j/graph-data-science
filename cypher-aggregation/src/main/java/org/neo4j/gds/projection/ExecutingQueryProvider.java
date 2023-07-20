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

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.StatementInfo;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Optional;

interface ExecutingQueryProvider {
    Optional<String> executingQuery();

    static ExecutingQueryProvider fromTransaction(Transaction transaction) {
        return new TxQuery(transaction);
    }

    static ExecutingQueryProvider empty() {
        return Optional::empty;
    }
}


final class TxQuery implements ExecutingQueryProvider {
    private final Transaction transaction;

    TxQuery(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Optional<String> executingQuery() {
        if (!(this.transaction instanceof InternalTransaction)) {
            return Optional.empty();
        }
        try (var statement = ((InternalTransaction) this.transaction).kernelTransaction().acquireStatement()) {
            if (!(statement instanceof StatementInfo)) {
                return Optional.empty();
            }

            return ((StatementInfo) statement).queryRegistry().executingQuery().flatMap(eq -> {
                return Optional.ofNullable(eq.rawQueryText());
//                return eq.snapshot().obfuscatedQueryText().or(() -> Optional.ofNullable(eq.rawQueryText()));
            });
        }
    }
}
