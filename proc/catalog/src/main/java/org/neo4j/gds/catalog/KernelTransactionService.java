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
package org.neo4j.gds.catalog;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

public class KernelTransactionService {
    private final Context context;

    public KernelTransactionService(Context context) {
        this.context = context;
    }

    public KernelTransaction getKernelTransaction() {
        try {
            //noinspection resource
            var itx = (InternalTransaction) context.transaction();

            return itx.kernelTransaction();
        } catch (ProcedureException e) {
            // This should never happen and the API makes us write this scaffolding
            throw new IllegalStateException("Unable to obtain kernel transaction", e);
        }
    }
}
