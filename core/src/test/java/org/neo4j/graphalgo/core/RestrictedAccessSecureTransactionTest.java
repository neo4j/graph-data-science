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

import org.junit.jupiter.api.Test;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class RestrictedAccessSecureTransactionTest extends SecureTransactionTestBase {

    @ExtensionCallback
    @Override
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(Settings.enterpriseLicensed(), true);
    }

    @Test
    void noNodeAccessAllowed() {
        var graph = noNodeAccessAllowedGraph();
        assertTrue(graph.isEmpty());
    }

    @Test
    void noRelationshipAccessAllowed() {
        var graph = noRelationshipAccessAllowedGraph();
        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0}), (b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @Test
    void noTargetNodeAccessAllowed() throws Exception {
        var graph = noTargetNodeAccessAllowedGraph();
        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0})"),
            graph
        );
    }

    @Test
    void noNodeProp1Allowed() throws Exception {
        var graph = noNodeProp1AllowedGraph();
        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 1.0, prop2: 2.0})-[{prop3: 23.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }

    @Test
    void noRelProp3Allowed() throws Exception {
        var graph = noRelProp3AllowedGraph();
        assertGraphEquals(
            fromGdl("(a:Node1 {prop1: 42.0, prop2: 2.0})-[{prop3: 3.0}]->(b:Node2 {prop1: 1.0, prop2: 1337.0})"),
            graph
        );
    }
}
