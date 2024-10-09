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

import com.carrotsearch.hppc.ObjectArrayList;

class PairingHeap {
    private PairingHeapElement root;
    private final ObjectArrayList<PairingHeapElement> helpingArray;

    public PairingHeap(ObjectArrayList<PairingHeapElement> helpingArray) {
        this.helpingArray = helpingArray;
        root = null;
    }


    public boolean empty() {
        return root == null;
    }

    public void add(long pairingId, double value) {
        root = addNewElement(root, pairingId, value);
    }

    public long minElement() {
        return root.pairingId();
    }

    public void increaseValues(double value) {
        if (root != null) {
            root.addOffset(value);
        }
    }

    public double minValue() {
        return root.value();
    }

    public void pop() {
        var offset = root.childrenOffset();
        root = deleteNonRecursive(root);
        if (root != null) {
            root.addOffset(offset);
        }
    }

    public PairingHeap join(PairingHeap other) {
        root = meld(root, other.root);
        return this;
    }

    private PairingHeapElement addNewElement(PairingHeapElement node, long pairingId, double value) {
        return meld(node, PairingHeapElement.create(pairingId, value));
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

        if (element1.value() > element2.value()) {
            parent = element2;
            child = element1;
        }
        child.addOffset(-parent.childrenOffset());
        parent.addChild(child);
        return parent;
    }

    private PairingHeapElement deleteNonRecursive(PairingHeapElement node) {
        int position = 0; //settle with int indexing for the moment
        var currentNode = node.left();
        helpingArray.elementsCount = 0;

        PairingHeapElement pre = null;
        while (currentNode != null) {

            var next = currentNode.next();
            currentNode.nullifyNext();

            if (pre != null) {
                var elementToAdd = meld(pre, currentNode); //do a meld on the fly, helps save some space
                helpingArray.add(elementToAdd);
                position++;
                pre = null;
            } else {
                pre = currentNode;
            }

            currentNode = next;
        }

        if (pre != null) {
            helpingArray.add(pre);
            position++;
        }

        if (position == 0) {
            return null;
        }

        var stillLeft = position;
        /*
         * all we need to do is to make is meld logn times
         * let us do it in the simplest way:
         * iteration:
         * if there are k elements left, meld a[0] with a[k-1], a[1] with a[k-2] etc
         * this guarantees all active elements remain at positions a[0]....a[k/2]
         */
        while (stillLeft > 1) {
            int j = stillLeft / 2;
            int out = stillLeft - 1;
            for (int i = 0; i < j; ++i) {
                helpingArray.set(i, meld(helpingArray.get(i), helpingArray.get(out - i)));
                stillLeft--;
            }
        }
        return helpingArray.get(0);
    }
}
