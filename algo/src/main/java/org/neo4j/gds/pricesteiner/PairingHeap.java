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
package org.neo4j.gds.pricesteiner;

public class PairingHeap {
   private  PairingHeapElement root;

    public PairingHeap() {
        root = null;
    }

    public boolean empty() {
        return root == null;
    }

    public void add(long  pairingId, double value) {
        root = addNewElement(root, pairingId, value);
    }

    public long minElement(){
        return root.pairingId();
    }

    public void  increaseValues(double value){
        root.addOffset(value);
    }
    public double minValue(){
        return root.value();
    }

    public void pop() {
        var offset = root.childrenOffset();
        root = delete(root);
        root.addOffset(offset);
    }

    public void join(PairingHeap other) {
        root = meld(root, other.root);
    }

    private PairingHeapElement addNewElement(PairingHeapElement node, long  pairingId, double value) {
        return meld(node, PairingHeapElement.create(pairingId,value));
    }

    private PairingHeapElement meld(PairingHeapElement element1, PairingHeapElement element2) {
        if (element1 == null) {
            return element2;
        }

        if (element2 == null) {
            return element1;
        }

        PairingHeapElement parent = element1;
        PairingHeapElement child = element2;

        if (element1.value() >  element2.value()) {
            parent = element2;
            child = element1;
        }
        child.addOffset(-parent.childrenOffset());
        parent.addChild(child);
        return parent;
    }
    private PairingHeapElement delete(PairingHeapElement node) {
        return mergePairs(node.left());
    }
    private  PairingHeapElement deleteNonRecursive(PairingHeapElement node) {
        var current = node.left();
        return null;
    }
    private PairingHeapElement mergePairs(PairingHeapElement node) {
        if (node == null || node.next() == null)
            return node;
        else {
           var  element = node;
           var  next = node.next();
           var  nextNext = node.next().next();

            element.nullifyNext();
            next.nullifyNext();

            return meld(meld(element, next), mergePairs(nextNext));
        }
    }

}
