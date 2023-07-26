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
package org.neo4j.gds.core.compression.mixed;

import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.compression.packed.Address;

public class MixedPage {

    private final AdjacencyListBuilder.Slice<Address> address;

    private final AdjacencyListBuilder.Slice<byte[]> bytes;

    public MixedPage() {
        this.address = ModifiableSlice.create();
        this.bytes = ModifiableSlice.create();
    }

    public AdjacencyListBuilder.Slice<Address> address() {
        return address;
    }

    public AdjacencyListBuilder.Slice<byte[]> bytes() {
        return bytes;
    }

    //    <R, P> R visit(MixedPageVisitor<R, P> visitor, P param);
//
//    class AddressPage implements MixedPage {
//
//        private final Address address;
//
//        public AddressPage(Address address) {this.address = address;}
//
//        @Override
//        public <R, P> R visit(MixedPageVisitor<R, P> visitor, P param) {
//            return visitor.visitAddress(address, param);
//        }
//    }
//
//    class BytesPage implements MixedPage {
//
//        private final byte[] bytes;
//
//        public BytesPage(byte[] bytes) {this.bytes = bytes;}
//
//        @Override
//        public <R, P> R visit(MixedPageVisitor<R, P> visitor, P param) {
//            return visitor.visitBytes(bytes, param);
//        }
//    }
}
