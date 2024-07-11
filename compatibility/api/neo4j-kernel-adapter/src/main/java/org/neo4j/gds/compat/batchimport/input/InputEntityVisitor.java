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
package org.neo4j.gds.compat.batchimport.input;

import org.neo4j.internal.id.IdSequence;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Receives calls for extracted data from {@link InputChunk}. This callback design allows for specific methods
 * using primitives and other optimizations, to avoid garbage.
 */
public interface InputEntityVisitor extends Closeable {
    boolean propertyId(long nextProp);

    boolean properties(ByteBuffer properties, boolean offloaded);

    boolean property(String key, Object value);

    boolean property(int propertyKeyId, Object value);

    // For nodes
    boolean id(long id);

    boolean id(Object id, Group group);

    boolean id(Object id, Group group, IdSequence idSequence);

    boolean labels(String[] labels);

    boolean labelField(long labelField);

    // For relationships
    boolean startId(long id);

    boolean startId(Object id, Group group);

    boolean endId(long id);

    boolean endId(Object id, Group group);

    boolean type(int type);

    boolean type(String type);

    void endOfEntity() throws IOException;

    void reset();

    class Adapter implements InputEntityVisitor {
        @Override
        public boolean property(String key, Object value) {
            return true;
        }

        @Override
        public boolean properties(ByteBuffer properties, boolean offloaded) {
            return true;
        }

        @Override
        public boolean property(int propertyKeyId, Object value) {
            return true;
        }

        @Override
        public boolean propertyId(long nextProp) {
            return true;
        }

        @Override
        public boolean id(long id) {
            return true;
        }

        @Override
        public boolean id(Object id, Group group) {
            return true;
        }

        @Override
        public boolean id(Object id, Group group, IdSequence idSequence) {
            return true;
        }

        @Override
        public boolean labels(String[] labels) {
            return true;
        }

        @Override
        public boolean startId(long id) {
            return true;
        }

        @Override
        public boolean startId(Object id, Group group) {
            return true;
        }

        @Override
        public boolean endId(long id) {
            return true;
        }

        @Override
        public boolean endId(Object id, Group group) {
            return true;
        }

        @Override
        public boolean type(int type) {
            return true;
        }

        @Override
        public boolean type(String type) {
            return true;
        }

        @Override
        public boolean labelField(long labelField) {
            return true;
        }

        @Override
        public void endOfEntity() {}

        @Override
        public void reset() {}

        @Override
        public void close() {}
    }

    InputEntityVisitor NULL = new Adapter() { // empty
            };
}
