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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.FilterAccessMode;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.Arrays;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

abstract class SecureTransactionTestBase extends BaseTest {

    @BeforeEach
    void setUp() {
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
        assertFullGraph(graph);
    }

    Graph noNodeAccessAllowedGraph() {
        AccessMode noNodesAllowed = new FilterAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return false;
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noNodesAllowed))
            .build()
            .graph();
    }

    Graph noRelationshipAccessAllowedGraph() {
        AccessMode noRelsAllowed = new FilterAccessMode() {
            @Override
            public boolean allowsTraverseRelType(int relType) {
                return false;
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noRelsAllowed))
            .build()
            .graph();
    }

    Graph noSourceNodeAccessAllowedGraph() throws Exception {
        int labelTokenNode1 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().labelGetOrCreateForName("Node1"));

        AccessMode noNode1Allowed = new FilterAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowTraverseAllNodesWithLabel(long label) {
                return !(label == labelTokenNode1);
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return Arrays.stream(labels).noneMatch(l -> l == labelTokenNode1);
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noNode1Allowed))
            .build()
            .graph();
    }

    Graph noTargetNodeAccessAllowedGraph() throws Exception {
        int labelTokenNode2 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().labelGetOrCreateForName("Node2"));

        AccessMode noNode2Allowed = new FilterAccessMode() {
            @Override
            public boolean allowsTraverseAllLabels() {
                return false;
            }

            @Override
            public boolean allowTraverseAllNodesWithLabel(long label) {
                return !(label == labelTokenNode2);
            }

            @Override
            public boolean allowsTraverseNode(long... labels) {
                return Arrays.stream(labels).noneMatch(l -> l == labelTokenNode2);
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, noNode2Allowed))
            .build()
            .graph();
    }

    Graph noNodeProp1AllowedGraph() throws Exception {
        int propertyKeyProp1 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().propertyKeyGetOrCreateForName("prop1"));

        AccessMode prop1NotAllowed = new FilterAccessMode() {

            @Override
            public boolean allowsReadNodeProperty(int propertyKey) {
                return propertyKey != propertyKeyProp1;
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, prop1NotAllowed))
            .build()
            .graph();
    }

    Graph noRelProp3AllowedGraph() throws Exception {
        int propertyKeyProp3 = SecureTransaction.of(db).apply((tx, ktx) ->
            ktx.tokenWrite().propertyKeyGetOrCreateForName("prop3"));

        AccessMode prop3NotAllowed = new FilterAccessMode() {
            @Override
            public boolean allowsReadRelationshipProperty(int propertyKey) {
                return propertyKey != propertyKeyProp3;
            }
        }.toNeoAccessMode();

        return storeLoaderBuilder()
            .securityContext(new SecurityContext(AuthSubject.ANONYMOUS, prop3NotAllowed))
            .build()
            .graph();
    }

    private @NotNull StoreLoaderBuilder storeLoaderBuilder() {
        return new StoreLoaderBuilder()
            .api(db)
            .concurrency(1)
            .addNodeLabels("Node1", "Node2")
            .addNodeProperty(PropertyMapping.of("prop1", 1))
            .addNodeProperty(PropertyMapping.of("prop2", 2))
            .addRelationshipType("REL")
            .addRelationshipProperty(PropertyMapping.of("prop3", 3));
    }

    void assertFullGraph(Graph graph) {
        var expected = fromGdl(
            "(a:Node1 {prop1: 42.0, prop2: 2.0})-[{prop3: 23.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"
        );
        assertGraphEquals(expected, graph);
    }
}
