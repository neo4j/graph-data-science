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

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class StatementApi {

    public interface TxConsumer {
        void accept(KernelTransaction transaction) throws KernelException;
    }

    public interface TxFunction<T> {
        T apply(KernelTransaction transaction) throws KernelException;
    }


    protected final GraphDatabaseAPI api;
    private final TransactionWrapper tx;

    protected StatementApi(GraphDatabaseAPI api) {
        this.api = api;
        this.tx = new TransactionWrapper(api);
    }

    protected final <T> T applyInTransaction(TxFunction<T> fun) {
        return tx.apply(ktx -> {
            try {
                return fun.apply(ktx);
            } catch (KernelException e) {
                return ExceptionUtil.throwKernelException(e);
            }
        });
    }

    protected final void acceptInTransaction(TxConsumer fun) {
        tx.accept(ktx -> {
            try {
                fun.accept(ktx);
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
        });
    }

    protected <T> T resolve(Class<T> dependency) {
        return api.getDependencyResolver().resolveDependency(dependency);
    }
}
