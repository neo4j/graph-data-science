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

class LinkCutNode {
    private LinkCutNode up;
    private LinkCutNode left;
    private LinkCutNode right;
    private final long source;//from;
    private boolean reverseBit;

    LinkCutNode(long source, LinkCutNode p) {
        up = p;
        left = null;
        right = null;
        this.source = source;
        reverseBit = false;
    }

    void reverseBit() {
        reverseBit = !reverseBit;
    }

    void setChild(LinkCutNode node, Direction direction) {
        if (direction == Direction.LEFT) {
            left = node;
        } else {
            right = node;
        }
    }

    boolean getReversedBit() {return reverseBit;}

    long source() {return source;}


    LinkCutNode parent() {return up;}

    LinkCutNode left() {return left;}

    LinkCutNode right() {return right;}

    LinkCutNode root() {
        LinkCutNode current = this;
        while (current.up != null) {
            current = current.up;
        }
        return current;
    }

    void setParent(LinkCutNode sn) {
        up = sn;
    }

    private boolean checkChild(LinkCutNode node) {
        if (node != null) {
            return node.equals(this);
        }
        return false;
    }

    boolean isChildOf(LinkCutNode node) {
        if (node == null) return false;
        return checkChild(node.left) || checkChild(node.right);
    }

    static LinkCutNode createSingle(long id) {
        return new LinkCutNode(id, null);
    }

}
