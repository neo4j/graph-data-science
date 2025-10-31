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
package org.neo4j.gds.procedures;

import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.integration.Neo4jPoweredRequestCorrelationId;
import org.neo4j.kernel.api.KernelTransaction;

public class RequestCorrelationIdAccessor {
    /**
     * We use the transaction sequence number not because we particularly want to, but because Neo4j has limitations.
     * The transaction id is not available, if you try to use it, you get this error:
     * "Transaction id is not assigned yet. It will be assigned during transaction commit";
     * and they do not offer a first class request id concept.
     * Here is to them improving that in future.
     */
    public RequestCorrelationId getRequestCorrelationId(KernelTransaction kernelTransaction) {
        return Neo4jPoweredRequestCorrelationId.create(kernelTransaction.getTransactionSequenceNumber());
    }
}
