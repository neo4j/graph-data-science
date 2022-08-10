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

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;

import static org.assertj.core.api.Assertions.assertThat;

class OpenGdsTransactionContextTest extends TransactionContextTest {

    @Test
    void noNodeAccessAllowed() {
        applyTxWithAccessMode(tx -> assertThat(tx.getAllNodes().stream().count()).isEqualTo(0L), forbiddenNodes());
    }

    @Test
    void noNode2LabelAccessAllowed() throws KernelException {
        applyTxWithAccessMode(tx -> assertThat(tx.getAllNodes().stream().count()).isEqualTo(1L), forbiddenLabel("Node2"));
    }

    @Test
    void noRelationshipAccessAllowed() {
        applyTxWithAccessMode(tx -> assertThat(tx.getAllRelationships().stream().count()).isEqualTo(0L), forbiddenRelationships());
    }

    @Test
    void noPropertyKeyAccessAllowed() throws KernelException {
        applyTxWithAccessMode(tx -> {
            var nodesWithProp1Count = tx.getAllNodes().stream().filter(node -> node.hasProperty("prop1")).count();
            assertThat(nodesWithProp1Count).isEqualTo(0L);
        }, forbiddenProperty("prop1"));
    }
}
