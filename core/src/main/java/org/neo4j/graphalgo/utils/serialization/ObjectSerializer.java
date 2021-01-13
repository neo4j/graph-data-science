/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.utils.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class ObjectSerializer {

    private ObjectSerializer() {}

    public static <T extends Serializable> byte[] toByteArray(T obj) throws IOException {
        try (
            var byteArrayOutputStream = new ByteArrayOutputStream();
            var outputStream = new ObjectOutputStream(byteArrayOutputStream)
        ) {
            outputStream.writeObject(obj);
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static <T extends Serializable> T fromByteArray(byte[] byteBuffer, Class<T> clazz) throws IOException, ClassNotFoundException {
        try (
            var byteArrayInputStream = new ByteArrayInputStream(byteBuffer);
            var inputStream = new ObjectInputStream(byteArrayInputStream)
        ) {
            return clazz.cast(inputStream.readObject());
        }
    }

    public static <T extends Serializable> T fromByteArrayUnsafe(byte[] byteBuffer) throws IOException, ClassNotFoundException {
        try (
            var byteArrayInputStream = new ByteArrayInputStream(byteBuffer);
            var inputStream = new ObjectInputStream(byteArrayInputStream)
        ) {
            return (T) inputStream.readObject();
        }
    }
}
