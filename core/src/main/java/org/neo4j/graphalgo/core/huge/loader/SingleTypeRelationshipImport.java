/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;

import java.util.stream.Stream;

final class SingleTypeRelationshipImport {

    private final RelationshipTypeMapping mapping;
    private final RelationshipImporter mainImporter;

    SingleTypeRelationshipImport(
            RelationshipTypeMapping mapping,
            RelationshipImporter mainImporter) {
        this.mapping = mapping;
        this.mainImporter = mainImporter;
    }

    WithImporter loadImporter(
            boolean loadAsUndirected,
            boolean loadOutgoing,
            boolean loadIncoming,
            boolean loadWeights) {
        RelationshipImporter.Imports imports = mainImporter.imports(
                loadAsUndirected,
                loadOutgoing,
                loadIncoming,
                loadWeights);
        if (imports == null) {
            return null;
        }
        return new WithImporter(imports);
    }

    class WithImporter {
        private final RelationshipImporter.Imports imports;

        WithImporter(RelationshipImporter.Imports imports) {
            this.imports = imports;
        }

        Stream<Runnable> flushTasks() {
            return mainImporter.flushTasks().stream();
        }

        WithBuffer withBuffer(IdMapping idMap, int bulkSize, Read read, CursorFactory cursors) {
            RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(idMap, mapping.typeId, bulkSize);
            RelationshipImporter.WeightReader weightReader = mainImporter.storeBackedWeightReader(cursors, read);
            return new WithBuffer(imports, weightReader, buffer);
        }
    }

    class WithBuffer {
        private final RelationshipImporter.Imports imports;
        private final RelationshipImporter.WeightReader weightReader;
        private final RelationshipsBatchBuffer buffer;

        WithBuffer(
                RelationshipImporter.Imports imports,
                RelationshipImporter.WeightReader weightReader,
                RelationshipsBatchBuffer buffer) {
            this.imports = imports;
            this.weightReader = weightReader;
            this.buffer = buffer;
        }

        RelationshipsBatchBuffer buffer() {
            return buffer;
        }

        long importRels() {
            return imports.importRels(buffer, weightReader);
        }
    }
}
