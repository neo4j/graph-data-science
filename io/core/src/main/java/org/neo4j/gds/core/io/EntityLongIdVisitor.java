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
package org.neo4j.gds.core.io;

import org.neo4j.gds.compat.batchimport.input.Group;
import org.neo4j.gds.compat.batchimport.input.InputEntityVisitor;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;

public interface EntityLongIdVisitor {

    void visitNodeId(InputEntityVisitor visitor, long id);

    void visitSourceId(InputEntityVisitor visitor, long id);

    void visitTargetId(InputEntityVisitor visitor, long id);

    EntityLongIdVisitor ACTUAL = new EntityLongIdVisitor() {
        @Override
        public void visitNodeId(InputEntityVisitor visitor, long id) {
            visitor.id(id);
        }

        @Override
        public void visitSourceId(InputEntityVisitor visitor, long id) {
            visitor.startId(id);
        }

        @Override
        public void visitTargetId(InputEntityVisitor visitor, long id) {
            visitor.endId(id);
        }
    };

    static EntityLongIdVisitor mapping(ReadableGroups readableGroups) {
        return new Mapping(readableGroups.get(null));
    }

    final class Mapping implements EntityLongIdVisitor {

        private final Group globalGroup;

        private Mapping(Group globalGroup) {this.globalGroup = globalGroup;}

        @Override
        public void visitNodeId(InputEntityVisitor visitor, long id) {
            visitor.id(id, globalGroup);
        }

        @Override
        public void visitSourceId(InputEntityVisitor visitor, long id) {
            visitor.startId(id, globalGroup);
        }

        @Override
        public void visitTargetId(InputEntityVisitor visitor, long id) {
            visitor.endId(id, globalGroup);
        }
    }
}
