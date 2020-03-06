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

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.ExecutingQuery;

import java.util.stream.Collectors;

public final class KernelTransactionsProxy {

    public static long lastTransactionIdWhenStarted(KernelTransactionHandle ktx) {
        return ktx.lastTransactionTimestampWhenStarted();
    }

    public static void markForTermination(KernelTransactionHandle ktx) {
        ktx.markForTermination(Transactions.markedAsFailed());
    }

    public static String executingQueryTexts(KernelTransactionHandle ktx, String delimiter) {
        return ktx.executingQueries()
            .map(ExecutingQuery::queryText)
            .collect(Collectors.joining(delimiter));
    }

    private KernelTransactionsProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
