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

public class KernelTransactionService {
    private final Context context;

    public KernelTransactionService(Context context) {
        this.context = context;
    }

    KernelTransaction getKernelTransaction() {
        try {
            // this is not opening something new that we should close
            // it only fetches the current transaction
            // compare to what Neo4j does in: https://github.com/neo-technology/neo4j/blame/5.7.0/public/community/neo4j/src/main/java/org/neo4j/graphdb/facade/DatabaseManagementServiceFactory.java#L368-L369
            // newer Neo4j versions offer a more direct method: https://github.com/neo-technology/neo4j/blob/e6a228f60efac4fd2584f5ec00de0207aad944ff/public/community/neo4j/src/main/java/org/neo4j/graphdb/facade/DatabaseManagementServiceFactory.java#L356
            // we should make sure we do this in a compatible way
            return context.internalTransaction().kernelTransaction();
        } catch (ProcedureException e) {
            // Neo4j itself throws a different exception here; see https://github.com/neo-technology/neo4j/blob/340b40bb956d077b3127c8ba8eb33f2d288bc844/public/community/procedure/src/main/java/org/neo4j/procedure/impl/FieldSetter.java#L44-L49
            throw new RuntimeException(e);
        }
    }
}
