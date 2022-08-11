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
package org.neo4j.gds.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.compat.FilterAccessMode;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Arrays;
import java.util.function.Consumer;

public abstract class TransactionContextTest extends BaseTest {

    @BeforeEach
    void setUp() {
        db.executeTransactionally(
            "CREATE (n:Node1 {prop1: 42}), (o:Node2 {prop2: 1337}), (n)-[:REL {prop3: 23}]->(o)"
        );
    }

    protected AccessMode forbiddenLabel(String label) throws KernelException {
        int forbiddenToken;
        try (var tx = db.beginTx()) {
            var ktx = GraphDatabaseApiProxy.kernelTransaction(tx);
            forbiddenToken = ktx.tokenWrite().labelGetOrCreateForName(label);
        }
        // forbid reading label token with this access mode
        return new FilterAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowTraverseAllNodesWithLabel(long label1) {
                return label1 != forbiddenToken;
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return Arrays.stream(labels).noneMatch(l -> l == forbiddenToken);
            }
        }.toNeoAccessMode();
    }

    protected AccessMode forbiddenNodes() {
        return

            new FilterAccessMode() {
                @Override
                public boolean allowsTraverseAllLabels() {
                    return false;
                }

                @Override
                public boolean allowTraverseAllNodesWithLabel(long label) {
                    return false;
                }

                @Override
                public boolean allowsTraverseNode(long... labels) {
                    return false;
                }
            }.toNeoAccessMode();
    }

    protected AccessMode forbiddenRelationships() {
        return new FilterAccessMode() {
            @Override
            public boolean allowsTraverseRelType(int relType) {
                return false;
            }
        }.toNeoAccessMode();
    }

    protected AccessMode forbiddenProperty(String property) throws KernelException {
        int forbiddenToken;
        try (var tx = db.beginTx()) {
            var ktx = GraphDatabaseApiProxy.kernelTransaction(tx);
            forbiddenToken = ktx.tokenWrite().propertyKeyGetOrCreateForName(property);
        }

        return new FilterAccessMode() {
            @Override
            public boolean allowsReadNodeProperty(int propertyKey) {
                return propertyKey != forbiddenToken;
            }
        }.toNeoAccessMode();
    }

    protected void applyTxWithAccessMode(Consumer<Transaction> txConsumer, AccessMode noNodesAllowed) {
        try (var topLevelTx = db.beginTx()) {
            var securityContext = Neo4jProxy.securityContext(
                "",
                AuthSubject.ANONYMOUS,
                noNodesAllowed,
                db.databaseName()
            );
            ((InternalTransaction) topLevelTx)
                .kernelTransaction()
                .overrideWith(securityContext);

            txConsumer.accept(topLevelTx);
        }
    }
}
