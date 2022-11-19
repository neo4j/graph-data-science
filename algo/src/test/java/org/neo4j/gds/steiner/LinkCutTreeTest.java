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
package org.neo4j.gds.steiner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LinkCutTreeTest {
    @Test
    void shouldWork() {
        LinkCutTree tree = new LinkCutTree(10);
        assertThat(tree.connected(0, 1)).isFalse();
        tree.link(0, 1);
        assertThat(tree.connected(0, 1)).isTrue();
        tree.link(1, 2);
        assertThat(tree.connected(0, 2)).isTrue();
        tree.link(0, 3);
        assertThat(tree.connected(2, 3)).isTrue();
        tree.link(3, 4);
        assertThat(tree.connected(2, 4)).isTrue();
        tree.delete(0, 1);
        assertThat(tree.connected(2, 4)).isFalse();
        tree.link(1, 4);
        assertThat(tree.connected(2, 4)).isTrue();


    }

}
