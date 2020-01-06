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
package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import java.util.Arrays;

/**
 * Data structure to store an undirected graph as a tree.
 * The indented usage is to add all nodes in their tree shape and then
 * remove certain relationships to form independent clusters that can each be
 * traversed.
 * <p>
 * The tree does not guarantee an integrity invariant while adding or
 * removing relationships. To verify a valid tree structure, call
 * {@link #verifyTreeIntegrity()}.
 * <p>
 * Memory space is O(n) where n is the capacity – the expected number of nodes –
 * and independent from the number of relationships added.<br>
 * To be more precise, two int arrays with {@code n} elements are allocated upfront.
 * Additions are O(1).<br>
 * Removals are O(1) is their best case and O(n) in their worst case
 * where n is the number of siblings on the level where the node is removed.
 * The average case is an amortized O(n/2) because removals require
 * a linear scan on all siblings <i>until</i> the target node is found.
 * A completely flat tree with only one level is the absolute worst case for
 * removals. Removing the very last node on such a tree is a O(n) operation
 * where n is the number of nodes inserted.
 * <p>
 * This tree is undirected, that is, the arguments to
 * {@link #addRelationship(int, int)} do not mean {@code source -> target}.
 * Though iteration uses the {@link RelationshipConsumer} as consumer interface,
 * it does not guarantee that the source and target ids always correspond to the
 * arguments to {@link #addRelationship(int, int)}.
 * For example, one might call {@code tree.addRelationship(1337, 42)} and later
 * receive a relation with {@code source}={@code 42} and {@code target}={@code 1337}.
 * <p>
 * Iteration in DFS consumes no additional memory, but is not stack safe.
 * Large or invalid trees may lead to StackOverflowExceptions.
 * BFS Iteration should always be save, but consumes additional memory to keep
 * a BitSet for visited nodes and the iteration queue.
 * Iteration is roughly in reverse insertion order, but this is an implementation
 * detail and not guaranteed.
 */
public class UndirectedTree {

    private static final int INVALID_NODE = -1;
    private static final long RELATION_ID_NOT_SUPPORTED = -1L;

    private final int[] children, siblings;
    private final int capacity;
    private int size;

    /**
     * Creates a new Tree that can hold up to {@code capacity} nodes.
     */
    public UndirectedTree(int capacity) {
        try {
            children = new int[capacity];
            siblings = new int[capacity];
        } catch (OutOfMemoryError | NegativeArraySizeException e) {
            IllegalArgumentException iae =
                    new IllegalArgumentException("Invalid capacity: " + capacity);
            iae.addSuppressed(e);
            throw iae;
        }
        Arrays.fill(children, INVALID_NODE);
        Arrays.fill(siblings, INVALID_NODE);
        this.capacity = capacity;
    }

    /**
     * Adds a new undirected relationship to the tree.
     * Adding a self relationship – where {@code node1 == node2} – is a noop.
     *
     * @throws ArrayIndexOutOfBoundsException if the root node is not supported by this tree
     */
    public void addRelationship(int node1, int node2) {
        if (node1 < node2) {
            addChild(node1, node2);
        } else if (node1 > node2) {
            addChild(node2, node1);
        }
        // do nothing on node1 == node2
    }

    /**
     * Removes a relationship.
     * Removing a self relationship – where {@code node1 == node2} – is a noop.
     * Removing a relationship that does not exist is a noop and does not fail.
     *
     * @throws ArrayIndexOutOfBoundsException if the root node is not supported by this tree
     */
    public void removeRelationship(int node1, int node2) {
        if (node1 < node2) {
            removeChild(node1, node2);
        } else if (node1 > node2) {
            removeChild(node2, node1);
        }
        // do nothing on node1 == node2
    }

    /**
     * @return the number of relationships stored
     */
    public int size() {
        return size;
    }

    /**
     * @return true iff there are no relationships stored
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return true iff there is at least one relationship stored
     */
    public boolean nonEmpty() {
        return size > 0;
    }

    /**
     * Iterate over all relationships in a BFS (breadth-first) manner.
     * BFS iteration is always stack safe, even for invalid trees.
     * BFS iteration consumes additional memory during iteration.
     * The {@code relationshipTypeId} parameter of the {@link RelationshipConsumer}
     * is always {@code -1} as relation IDs are not supported by this tree.
     *
     * @throws ArrayIndexOutOfBoundsException if the root node is not supported by this tree
     */
    public void forEachBFS(int root, RelationshipConsumer consumer) {
        iterateBFS(root, consumer);
    }

    /**
     * Iterate over all relationships in a DFS (depth-first) manner.
     * DFS iteration are garbage-free but will consume at least two stack per
     * tree level – one for the level itself and at least one more for calling the
     * consumer. On very large trees and particularly on invalid trees, a DFS
     * might throw a {@link StackOverflowError}.
     * The {@code relationshipTypeId} parameter of the {@link RelationshipConsumer}
     * is always {@code -1} as relation IDs are not supported by this tree.
     *
     * @throws ArrayIndexOutOfBoundsException if the root node is not supported by this tree
     */
    public void forEachDFS(int root, RelationshipConsumer consumer) {
        iterateDFS(root, consumer);
    }

    /**
     * Verifies that the tree is in a valid shape.
     *
     * @throws IllegalStateException for invalid trees.
     */
    public void verifyTreeIntegrity() {
        BitSet seen = new BitSet(capacity);
        for (int child : children) {
            if (child != INVALID_NODE) {
                if (seen.getAndSet(child)) {
                    throw new IllegalStateException("node (" + child + ") has multiple parents");
                }
            }
        }
        for (int sibling : siblings) {
            if (sibling != INVALID_NODE) {
                if (seen.getAndSet(sibling)) {
                    throw new IllegalStateException("node (" + sibling + ") has multiple parents");
                }
            }
        }
    }

    private void addChild(int node1, int node2) {
        int sibling = children[node1];
        if (sibling != node2) {
            if (sibling != INVALID_NODE) {
                if (siblings[node2] != INVALID_NODE) {
                    throw new IllegalStateException("node (" + node2 + ") already has a parent");
                }
                siblings[node2] = sibling;
            }
            children[node1] = node2;
            size++;
        }
    }

    private void removeChild(int node1, int node2) {
        int sibling = children[node1];
        if (sibling == node2) {
            children[node1] = siblings[node2];
            siblings[node2] = INVALID_NODE;
            size--;
        } else {
            removeSibling(sibling, node2);
        }
    }

    private void removeSibling(int node1, int node2) {
        int next;
        while ((next = siblings[node1]) != INVALID_NODE) {
            if (next == node2) {
                siblings[node1] = siblings[next];
                siblings[node2] = INVALID_NODE;
                size--;
                break;
            }
            node1 = next;
        }
    }

    private void iterateBFS(int startNode, RelationshipConsumer consumer) {
        IntArrayDeque queue = new IntArrayDeque();
        BitSet seen = new BitSet(capacity);
        queue.addFirst(startNode);
        while (!queue.isEmpty()) {
            int node = queue.removeFirst();
            int child = children[node];
            if (child != INVALID_NODE) {
                do {
                    if (!seen.getAndSet(child)) {
                        queue.addLast(child);
                    }
                    if (!consumer.accept(node, child)) {
                        return;
                    }
                } while ((child = siblings[child]) != INVALID_NODE);
            }
        }
    }

    private void iterateDFS(int node, RelationshipConsumer consumer) {
        int child = children[node];
        if (child != INVALID_NODE) {
            int sibling = child;
            do {
                if (!consumer.accept(node, sibling)) {
                    return;
                }
                iterateDFS(sibling, consumer);
            }
            while ((sibling = siblings[sibling]) != INVALID_NODE);
        }
    }

    public String toString(int root) {
        final StringBuilder builder = new StringBuilder();
        iterateBFS(root, (s, t) -> {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(s).append(" -> ").append(t);
            return true;
        });
        return builder.toString();
    }
}
