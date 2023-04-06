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
package org.neo4j.gds.compat._56;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class CompatGraphDatabaseAPIImpl extends GdsGraphDatabaseAPI {

    CompatGraphDatabaseAPIImpl(DatabaseManagementService dbms) {
        super(dbms);
    }

    @Override
    public boolean isAvailable() {
        return api.isAvailable();
    }

    @Override
    public TopologyGraphDbmsModel.HostedOnMode mode() {
        // NOTE: This means we can never start clusters locally, which is probably fine since:
        //   1) We never did this before
        //   2) We only use this for tests and benchmarks
        return TopologyGraphDbmsModel.HostedOnMode.SINGLE;
    }

    @Override
    public InternalTransaction beginTransaction(
        KernelTransaction.Type type,
        LoginContext loginContext,
        ClientConnectionInfo clientInfo,
        long timeout,
        TimeUnit unit,
        Consumer<Status> terminationCallback,
        TransactionExceptionMapper transactionExceptionMapper
    ) {
        return api.beginTransaction(
            type,
            loginContext,
            clientInfo,
            timeout,
            unit,
            terminationCallback,
            transactionExceptionMapper
        );
    }
}
