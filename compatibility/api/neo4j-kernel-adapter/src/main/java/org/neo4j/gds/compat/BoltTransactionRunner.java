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
package org.neo4j.gds.compat;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.virtual.MapValue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class BoltTransactionRunner<SUBSCRIBER, BOOKMARK> {

    public int runQuery(
        String query,
        MapValue parameters,
        BoltTransaction boltTransaction,
        Function<QueryStatistics, Integer> statExtractor
    ) throws KernelException {
        var querySubscriber = boltQuerySubscriber();
        executeQuery(boltTransaction, query, parameters, querySubscriber.innerSubscriber());
        querySubscriber.assertSucceeded();
        boltTransaction.commit();
        return statExtractor.apply(querySubscriber.queryStatistics());
    }

    public BoltTransaction beginBoltWriteTransaction(
        BoltGraphDatabaseServiceSPI fabricDb,
        LoginContext loginContext
    ) {
        return beginBoltWriteTransaction(
            fabricDb,
            loginContext,
            KernelTransaction.Type.IMPLICIT,
            ClientConnectionInfo.EMBEDDED_CONNECTION,
            List.of(),
            Duration.ZERO, Map.of()
        );
    }

    protected abstract BoltQuerySubscriber<SUBSCRIBER> boltQuerySubscriber();

    protected abstract void executeQuery(
        BoltTransaction boltTransaction,
        String query,
        MapValue parameters,
        SUBSCRIBER querySubscriber
    ) throws QueryExecutionKernelException;

    protected abstract BoltTransaction beginBoltWriteTransaction(
        BoltGraphDatabaseServiceSPI fabricDb,
        LoginContext loginContext,
        KernelTransaction.Type kernelTransactionType,
        ClientConnectionInfo clientConnectionInfo,
        List<BOOKMARK> bookmarks,
        Duration txTimeout,
        Map<String, Object> txMetadata
    );
}
