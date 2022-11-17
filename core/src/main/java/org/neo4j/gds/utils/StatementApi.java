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
package org.neo4j.gds.utils;

import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.api.KernelTransaction;

public abstract class StatementApi {

    public interface TxConsumer {
        void accept(KernelTransaction transaction) throws Exception;
    }

    public interface TxFunction<T> {
        T apply(KernelTransaction transaction) throws Exception;
    }

    protected final TransactionContext tx;

    protected StatementApi(TransactionContext tx) {
        this.tx = tx;
    }

    protected final <T> T applyInTransaction(TxFunction<T> fun) {
        try {
            return tx.apply((tx, ktx) -> fun.apply(ktx));
        } catch (Exception e) {
            ExceptionUtil.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    protected final void acceptInTransaction(TxConsumer fun) {
        try {
            tx.accept((tx, ktx) -> fun.accept(ktx));
        } catch (Exception e) {
            ExceptionUtil.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    protected final int getOrCreateNodeLabelToken(String nodeLabel) {
        return applyInTransaction(stmt -> stmt
            .tokenWrite()
            .labelGetOrCreateForName(nodeLabel));
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
