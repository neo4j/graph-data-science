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

import org.neo4j.gds.core.utils.paged.HugeObjectArray;

class LinkCutTree {
//Look here: https://dl.acm.org/doi/pdf/10.1145/3828.3835
        HugeObjectArray<LinkCutNode> nodes;
        HugeObjectArray<LinkCutNode> edgeInTree;

        public LinkCutTree(long nodeCount) {
            nodes = HugeObjectArray.newArray(LinkCutNode.class,nodeCount);
            for (long i = 0; i < nodeCount; i++) {
                nodes.set(i, new LinkCutNode(i, i, null));
            }
            edgeInTree = HugeObjectArray.newArray(LinkCutNode.class,nodeCount);
        }
        private void fixSituation(LinkCutNode u) {
            singleNodeFix(u.parent());
            singleNodeFix(u);
        }
        private void fixAndSingle(LinkCutNode node){
            fixSituation(node);
            singleRotation(node);
        }
        private void zig(LinkCutNode u) {
            fixAndSingle(u.parent());
            fixAndSingle(u);
        }
        private void zag(LinkCutNode u) {
            fixAndSingle(u);
        }

        private void singleRotation(LinkCutNode u) {
            LinkCutNode a = u.left();
            LinkCutNode b = u.right();
            LinkCutNode p = u.parent();
            LinkCutNode pp = p.parent();

            boolean dirLeft = false;
            if (p.left() != null) {
                if (p.left().equals(u)) dirLeft = true;
            }
            boolean pch = false;
            if (pp != null) {
                pch = (p.isChildOf(pp) == true);
            }
            if (dirLeft) {
                addChild(p, b,Direction.LEFT);
                addChild(u, p,Direction.RIGHT);
            } else {
                addChild(p, a,Direction.RIGHT);
                addChild(u, p,Direction.LEFT);
            }
            u.setParent(pp);
            if (pch) {
                addChild(pp, u, (pp.right() == p) ? Direction.RIGHT : Direction.LEFT);
            }
        }

        private void singleNodeFix(LinkCutNode u) {

            if (u.getReversedBit()) {
                LinkCutNode left = u.left();
                LinkCutNode right = u.right();
                if (left != null) {
                    left.reverseBit();
                }
                if (right != null) {
                    right.reverseBit();
                }
                addChild(u, right,Direction.LEFT);
                addChild(u, left,Direction.RIGHT);
                u.reverseBit();
            }
        }



        private void expose(LinkCutNode u) {
            LinkCutNode last = null;
            for (LinkCutNode cy = u; cy != null; cy = cy.parent()) {
                splay(cy);
                addChild(cy, last,Direction.RIGHT);
                last = cy;
            }
            splay(u);
        }

        private void splay(LinkCutNode u) {
            while (true) {
                if (u.parent() == null) break;
                if (!u.isChildOf(u.parent())) break;
                Rotation info = getRotationInfo(u);
                if (info == Rotation.SINGLE) {
                    fixAndSingle(u);
                } else if (info == Rotation.ZIGZIG) {
                    zig(u);
                } else {
                    zag(u);
                }
            }
            singleNodeFix(u);
        }

        /*0: splice
         *1: single-rotation
         *2: zig-zig
         *3:ziz-zag
         *
         */
        private Rotation getRotationInfo(LinkCutNode sn) {
            LinkCutNode parent = sn.parent();
            if (parent.parent() == null) {
                return Rotation.SINGLE;
            } else {
                if (!parent.isChildOf(parent.parent())) {
                    return Rotation.SINGLE;
                }
                LinkCutNode pparent = parent.parent();
                if (parent.left() == sn) {
                    if (pparent.left() == parent) {
                        return Rotation.ZIGZIG;
                    }
                    return Rotation.ZIGZAG;
                } else {
                    if (pparent.right() == parent) {
                        return Rotation.ZIGZIG;
                    }
                    return Rotation.ZIGZIG;
                }
            }
        }

        private void addChild(LinkCutNode a, LinkCutNode b,Direction direction) {
            a.setChild(b,direction);
            if (b != null) {
                b.setParent(a);
            }
        }

        public void link(long source, long target) {
            LinkCutNode nodeSource = nodes.get(source);
            LinkCutNode nodeTarget = nodes.get(target);
            LinkCutNode nodeEdge = new LinkCutNode(source, target, null);
            edgeInTree.set(target,nodeEdge);
            nodeEdge.setParent(nodeSource);
            evert(target);
            nodeTarget.setParent(nodeEdge);
        }

        private void evert(long nodeId) {
            LinkCutNode na = nodes.get(nodeId);
            expose(na);
            na.reverseBit();
        }

        public boolean contains(long source, long target) {
            var  edge=edgeInTree.get(target);
            return (edge==null) ? false : edge.source() == source;
        }

        public boolean connected(long source,long target){
            LinkCutNode nodeSource=nodes.get(source);
            LinkCutNode nodeTarget=nodes.get(target);
            if (nodeTarget == null || nodeTarget ==null){
                return false;
            }
            expose(nodeSource);
            return nodeSource.equals(nodeTarget.root());
        }

        private void cut(long source, long target) {
            LinkCutNode node;
            if (source != target) {
                node = edgeInTree.get(target);
            } else {
                node = nodes.get(source);
            }
            expose(node);
            singleNodeFix(node);
            if (node.left() != null) {
                LinkCutNode l = node.left();
                addChild(node, null,Direction.LEFT);
                l.setParent(null);
            }
        }

        public void delete(long source, long target) {
            evert(source);
            cut(source, target);
            cut(target, target);
            edgeInTree.set(target,null);
        }

}
