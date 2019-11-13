/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.container;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

final class UndirectedTreeTest {

    @Test
    void shouldFailForNegativeCapacity() {
        try {
            new UndirectedTree(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid capacity: -1", e.getMessage());
        }
    }

    @Test
    void shouldFailForTooLargeCapacity() {
        try {
            new UndirectedTree(Integer.MAX_VALUE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "Invalid capacity: " + Integer.MAX_VALUE,
                    e.getMessage());
        }
    }

    @Test
    void shouldNotResizeDynamically() {
        try {
            new UndirectedTree(1)
                    .addRelationship(0, 1);
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "Cannot operate on node (1) with capacity 1",
                    e.getMessage());
        }
    }

    @Test
    void shouldDiscardSelfRelationships() {
        UndirectedTree tree = new UndirectedTree(0);
        tree.addRelationship(0, 0);
        assertEquals(0, tree.size());
    }

    @Test
    void shouldAddSingleRelationshipRegardlessOfOrder() {
        UndirectedTree tree = new UndirectedTree(2);
        tree.addRelationship(0, 1);
        tree.addRelationship(1, 0);
        assertEquals(1, tree.size());
        tree.forEachBFS(0, (src, tgt) -> {
            assertEquals(0, src, "source should be 0");
            assertEquals(0, src, "target should be 1");
            return true;
        });
    }

    @Test
    void testLinearTree() {
        UndirectedTree tree = new UndirectedTree(4);
        tree.addRelationship(0, 1);
        tree.addRelationship(2, 3);
        tree.addRelationship(2, 1);

        testDfs(tree, "0|1", "1|2", "2|3");
        testBfs(tree, "0|1", "1|2", "2|3");
    }

    @Test
    void testSimpleBranchTree() {
        UndirectedTree tree = new UndirectedTree(4);
        tree.addRelationship(0, 2);
        tree.addRelationship(1, 0);
        tree.addRelationship(1, 3);

        testDfs(tree, "0|1", "1|3", "0|2");
        testBfs(tree, "0|1", "0|2", "1|3");
    }

    @Test
    void testLargerTree() {
        UndirectedTree tree = new UndirectedTree(12);
        tree.addRelationship(0, 1);
        tree.addRelationship(0, 4);
        tree.addRelationship(0, 8);
        tree.addRelationship(1, 2);
        tree.addRelationship(4, 5);
        tree.addRelationship(4, 7);
        tree.addRelationship(8, 9);
        tree.addRelationship(2, 3);
        tree.addRelationship(5, 6);
        tree.addRelationship(9, 10);
        tree.addRelationship(9, 11);

        testDfs(
                tree,
                "0|8",
                "8|9",
                "9|11",
                "9|10",
                "0|4",
                "4|7",
                "4|5",
                "5|6",
                "0|1",
                "1|2",
                "2|3");
        testBfs(
                tree,
                "0|8",
                "0|4",
                "0|1",
                "8|9",
                "4|7",
                "4|5",
                "1|2",
                "9|11",
                "9|10",
                "5|6",
                "2|3");
    }

    @Test
    void testRemoval() {
        UndirectedTree tree = new UndirectedTree(12);
        tree.addRelationship(0, 1);
        tree.addRelationship(0, 4);
        tree.addRelationship(0, 8);
        tree.addRelationship(1, 2);
        tree.addRelationship(4, 5);
        tree.addRelationship(4, 7);
        tree.addRelationship(8, 9);
        tree.addRelationship(2, 3);
        tree.addRelationship(5, 6);
        tree.addRelationship(9, 10);
        tree.addRelationship(9, 11);

        tree.removeRelationship(0, 8);
        tree.removeRelationship(4, 7);

        testBfs(tree, 0, "0|4", "0|1", "4|5", "1|2", "5|6", "2|3");
        testBfs(tree, 8, "8|9", "9|11", "9|10");
        testBfs(tree, 7);
    }

    @Test
    void shouldReportInvalidTrees() {
        UndirectedTree tree = new UndirectedTree(6);
        tree.addRelationship(0, 1);
        tree.addRelationship(0, 2);
        tree.addRelationship(1, 3);
        tree.addRelationship(2, 4);
        tree.addRelationship(4, 5);

        try {
            tree.addRelationship(1, 2);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("node (2) already has a parent", e.getMessage());
        }
    }

    private static void testBfs(
            UndirectedTree tree,
            int root,
            String... expected) {
        assertArrayEquals(expected, bfs(tree, root).toArray());
    }

    private static void testBfs(UndirectedTree tree, String... expected) {
        assertArrayEquals(expected, bfs(tree, 0).toArray());
    }

    private static void testDfs(UndirectedTree tree, String... expected) {
        assertArrayEquals(expected, dfs(tree, 0).toArray());
    }

    private static List<String> bfs(UndirectedTree tree, int root) {
        List<String> actual = new ArrayList<>();
        tree.forEachBFS(root, (src, tgt) -> {
            actual.add(src + "|" + tgt);
            return true;
        });
        return actual;
    }

    private static List<String> dfs(UndirectedTree tree, int root) {
        List<String> actual = new ArrayList<>();
        tree.forEachDFS(root, (src, tgt) -> {
            actual.add(src + "|" + tgt);
            return true;
        });
        return actual;
    }
}
