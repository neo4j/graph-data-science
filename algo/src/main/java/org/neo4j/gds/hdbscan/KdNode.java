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
package org.neo4j.gds.hdbscan;

// TODO: think if we actually  need   leftChild,rightChild, parent, or we can follow a implicit structure to store in array

import java.util.Optional;

class  KdNode{
    private final long start;
    private final long end;
    private final AABB aabb;
    private final boolean isLeaf;
    private  KdNode leftChild;
    private  KdNode rightChild;
    private  KdNode parent;
    private  KdNode sibling;
    private Optional<SplitInformation> splitInformation;

    static  KdNode createLeaf(long start, long end, AABB aabb){
        return new KdNode(start,end,aabb,true, Optional.empty());
    }

    static KdNode createSplitNode(long start, long end, AABB aabb, SplitInformation splitInformation){
        return  new KdNode(start,end,aabb,false, Optional.of(splitInformation));
    }

    private KdNode(long start, long end, AABB aabb, boolean isLeaf, Optional<SplitInformation> splitInformation) {
        this.start = start;
        this.end = end;
        this.aabb = aabb;
        this.isLeaf = isLeaf;
        this.splitInformation = splitInformation;
    }

    void parent(KdNode parent){
        this.parent = parent;
    }

    void sibling(KdNode sibling){
        this.sibling = sibling;
    }

    void leftChild(KdNode child){
        this.leftChild = child;
    }

    void rightChild(KdNode child){
        this.rightChild = child;
    }

    KdNode parent(){return parent;}

    KdNode rightChild(){return rightChild;}

    KdNode leftChild(){return leftChild;}

    KdNode sibling(){return sibling;}

    long start() { return  start;}

    long end() { return  end;}

    AABB aabb() { return  aabb;}

    boolean isLeaf() { return  isLeaf;}

    SplitInformation splitInformation(){
        return splitInformation.orElseThrow();
    }
}
