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

 class PairingHeapElement {

    private long  pairingId;
    private double value;
    private PairingHeapElement left;
    private PairingHeapElement next;
    private  double childrenOffset;

    static PairingHeapElement create(long pairingId, double value) {
        return  new PairingHeapElement(pairingId, value,null,null);
    }
    private PairingHeapElement(long pairingId, double value,PairingHeapElement left, PairingHeapElement next) {
        this.pairingId = pairingId;
        this.value = value;
        this.left = left;
        this.next = next;
    }

    void  addOffset(double v){
        childrenOffset += v;
        value+=v;
    }

    double childrenOffset(){
        return childrenOffset;
    }

    void addChild(PairingHeapElement child) {
         if (left != null) {
            child.next = left;
            }
         left = child;
    }

    long pairingId() {
        return pairingId;
    }

    double value() {
        return value;
    }

    PairingHeapElement left() { return left; }
     PairingHeapElement next() { return next; }

     void nullifyNext(){
        next = null;
     }

 }
