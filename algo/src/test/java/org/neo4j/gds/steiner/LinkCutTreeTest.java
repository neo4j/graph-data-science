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
    void shouldWorkInLine() {
        LinkCutTree tree = new LinkCutTree(10);
        assertThat(tree.connected(0, 1)).isFalse();
        tree.link(0, 1);  //0---1

        assertThat(tree.connected(0, 1)).isTrue();
        assertThat(tree.connected(1, 0)).isTrue();

        tree.link(1, 2); //0--1---2

        assertThat(tree.connected(0, 2)).isTrue();
        assertThat(tree.connected(2, 0)).isTrue();

        tree.link(0, 3); //3--0--1--2

        assertThat(tree.connected(2, 3)).isTrue();

        tree.link(3, 4);  //4--3--0--1--2

        assertThat(tree.connected(2, 4)).isTrue();

        tree.delete(0, 1); //4--3--0   1--2

        assertThat(tree.connected(2, 4)).isFalse();

        tree.link(1, 4);  //  2--1--4--3--0

        assertThat(tree.connected(2, 4)).isTrue();

    }

    @Test
    void shouldWorkInBinaryTree() {
        LinkCutTree tree = new LinkCutTree(11);
        tree.link(0, 1);
        tree.link(1, 3);
        tree.link(6, 9);
        tree.link(6, 10);
        tree.link(1, 4);
        tree.link(2, 5);
        tree.link(2, 6);
        tree.link(3, 7);
        tree.link(3, 8);

        assertThat(tree.connected(7, 10)).isFalse();

        tree.link(0, 2);

        assertThat(tree.connected(7, 10)).isTrue();
        assertThat(tree.connected(3, 2)).isTrue();

        tree.delete(0, 2);

        assertThat(tree.connected(7, 10)).isFalse();
        assertThat(tree.connected(3, 2)).isFalse();
        assertThat(tree.connected(7, 0)).isTrue();
        assertThat(tree.connected(9, 5)).isTrue();
        assertThat(tree.connected(7, 4)).isTrue();

        tree.delete(1, 3);

        assertThat(tree.connected(0, 4)).isTrue();
        assertThat(tree.connected(7, 8)).isTrue();
        assertThat(tree.connected(7, 4)).isFalse();
        assertThat(tree.connected(9, 5)).isTrue();

        tree.delete(6, 9);

        assertThat(tree.connected(9, 5)).isFalse();
        assertThat(tree.connected(2, 5)).isTrue();
        assertThat(tree.connected(2, 6)).isTrue();

        tree.link(0, 7);
        tree.link(0, 10);

        assertThat(tree.connected(2, 8)).isTrue();


    }
}
