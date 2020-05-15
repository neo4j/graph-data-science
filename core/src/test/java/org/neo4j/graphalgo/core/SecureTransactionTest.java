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
package org.neo4j.graphalgo.core;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.StoreScanner;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;

import java.util.Arrays;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class SecureTransactionTest extends BaseTest {

    @BeforeEach
    void setUp() {
        StoreScanner.useKernelCursors(true);
        db.executeTransactionally(
            "CREATE (n:Node1 {prop1: 42}), (o:Node2 {prop2: 1337}), (n)-[:REL {prop3: 23}]->(o)"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }


    @Test
    void defaultIsFullAccess() {
        var graph = storeLoaderBuilder().securityContext(SecurityContext.AUTH_DISABLED).build().graph();
        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0})-[w {prop3: 23.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @Test
    void noNodeAccessAllowed() {
        AccessMode noNodesAllowed = new FilteredAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return false;
            }
        };

        var graph = storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noNodesAllowed))
            .build()
            .graph();

        assertTrue(graph.isEmpty());
    }

    @Test
    void noRelationshipAccessAllowed() {
        AccessMode noRelsAllowed = new FilteredAccessMode() {
            @Override
            public boolean allowsTraverseRelType(int relType) {
                return false;
            }
        };

        var graph = storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noRelsAllowed))
            .build()
            .graph();

        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0}), (b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @Test
    void noTargetNodeAccessAllowed() throws Exception {
        int labelTokenNode2 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().labelGetOrCreateForName("Node2"));

        AccessMode noNode2Allowed = new FilteredAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return Arrays.stream(labels).noneMatch(l -> l == labelTokenNode2);
            }
        };

        var graph = storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noNode2Allowed))
            .build()
            .graph();

        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0})"),
            graph
        );
    }

    @Test
    void noNodeProp1Allowed() throws Exception {
        int propertyKeyProp1 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().propertyKeyGetOrCreateForName("prop1"));

        AccessMode prop1NotAllowed = new FilteredAccessMode() {
            @Override
            public boolean allowsReadNodeProperty(Supplier<LabelSet> labels, int propertyKey) {
                return propertyKey != propertyKeyProp1;
            }
        };

        var graph = storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, prop1NotAllowed))
            .build()
            .graph();

        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 1.0, prop2: 2.0})-[w {prop3: 23.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @Test
    void noRelProp3Allowed() throws Exception {
        int propertyKeyProp3 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().propertyKeyGetOrCreateForName("prop3"));

        AccessMode prop3NotAllowed = new FilteredAccessMode() {
            @Override
            public boolean allowsReadRelationshipProperty(IntSupplier relType, int propertyKey) {
                return propertyKey != propertyKeyProp3;
            }
        };

        var graph = storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, prop3NotAllowed))
            .build()
            .graph();

        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0})-[w {prop3: 3.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @NotNull
    private StoreLoaderBuilder storeLoaderBuilder() {
        return new StoreLoaderBuilder()
            .api(db)
            .concurrency(1)
            .addNodeLabels("Node1", "Node2")
            .addNodeProperty(PropertyMapping.of("prop1", 1))
            .addNodeProperty(PropertyMapping.of("prop2", 2))
            .addRelationshipType("REL")
            .addRelationshipProperty(PropertyMapping.of("prop3", 3));
    }

    static class FilteredAccessMode extends RestrictedAccessMode {
        FilteredAccessMode() {
            super(Static.FULL, Static.FULL);
        }
    }
}
