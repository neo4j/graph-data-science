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
package org.neo4j.graphalgo.utils;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.withKernelTransaction;
import static org.neo4j.graphalgo.utils.ExceptionUtil.throwIfUnchecked;

public abstract class StatementApi {

    public interface TxConsumer {
        void accept(KernelTransaction transaction) throws Exception;
    }

    public interface TxFunction<T> {
        T apply(KernelTransaction transaction) throws Exception;
    }

    protected final GraphDatabaseAPI api;

    protected StatementApi(GraphDatabaseAPI api) {
        this.api = api;
    }

    protected final <T> T applyInTransaction(TxFunction<T> fun) {
        return withKernelTransaction(api, ktx -> {
            try {
                return fun.apply(ktx);
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        });
    }

    protected final void acceptInTransaction(TxConsumer fun) {
        withKernelTransaction(api, ktx -> {
            try {
                fun.accept(ktx);
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    protected final int getOrCreatePropertyToken(String propertyKey) {
        return applyInTransaction(stmt -> stmt
            .tokenWrite()
            .propertyKeyGetOrCreateForName(propertyKey));
    }

    protected final int getOrCreateRelationshipToken(String relationshipType) {
        return applyInTransaction(stmt -> stmt
            .tokenWrite()
            .relationshipTypeGetOrCreateForName(relationshipType));
    }
}
