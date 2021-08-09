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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.IdMapping;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class SingleTypeRelationshipImporter {

    private final RelationshipImporter.Imports imports;
    private final RelationshipImporter.PropertyReader propertyReader;
    private final RelationshipsBatchBuffer buffer;

    private SingleTypeRelationshipImporter(
            RelationshipImporter.Imports imports,
            RelationshipImporter.PropertyReader propertyReader,
            RelationshipsBatchBuffer buffer) {
        this.imports = imports;
        this.propertyReader = propertyReader;
        this.buffer = buffer;
    }

    public RelationshipsBatchBuffer buffer() {
        return buffer;
    }

    public long importRelationships() {
        return imports.importRelationships(buffer, propertyReader);
    }

    public static class Builder {

        private final RelationshipType relationshipType;
        private final RelationshipProjection projection;
        private final RelationshipImporter importer;
        private final LongAdder relationshipCounter;
        private final int typeId;
        private final boolean validateRelationships;
        private final boolean loadProperties;

        public Builder(
            RelationshipType relationshipType,
            RelationshipProjection projection,
            boolean loadProperties,
            int typeToken,
            RelationshipImporter importer,
            LongAdder relationshipCounter,
            boolean validateRelationships
        ) {
            this.relationshipType = relationshipType;
            this.projection = projection;
            this.typeId = typeToken;
            this.importer = importer;
            this.relationshipCounter = relationshipCounter;
            this.loadProperties = loadProperties && projection.properties().hasMappings();
            this.validateRelationships = validateRelationships;
        }

        RelationshipType relationshipType() {
            return relationshipType;
        }

        LongAdder relationshipCounter() {
            return relationshipCounter;
        }

        boolean loadProperties() {
            return this.loadProperties;
        }

        public WithImporter loadImporter(boolean loadProperties) {
            RelationshipImporter.Imports imports = importer.imports(projection.orientation(), loadProperties);
            return new WithImporter(imports);
        }

        public class WithImporter {
            private final RelationshipImporter.Imports imports;

            WithImporter(RelationshipImporter.Imports imports) {
                this.imports = imports;
            }

            public Stream<Runnable> flushTasks() {
                return importer.flushTasks().stream();
            }

            public SingleTypeRelationshipImporter withBuffer(
                IdMapping idMap,
                int bulkSize,
                RelationshipImporter.PropertyReader propertyReader
            ) {
                RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(
                    idMap.cloneIdMapping(),
                    typeId,
                    bulkSize,
                    validateRelationships
                );
                return new SingleTypeRelationshipImporter(imports, propertyReader, buffer);
            }

            SingleTypeRelationshipImporter withBuffer(
                IdMapping idMap,
                int bulkSize,
                KernelTransaction kernelTransaction
            ) {
                RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(
                    idMap.cloneIdMapping(),
                    typeId,
                    bulkSize,
                    validateRelationships
                );
                RelationshipImporter.PropertyReader propertyReader = loadProperties
                    ? importer.storeBackedPropertiesReader(kernelTransaction)
                    : (relationshipReferences, propertyReferences, numberOfReferences, propertyKeyIds, defaultValues, aggregations, atLeastOnePropertyToLoad) -> new long[propertyKeyIds.length][0];
                return new SingleTypeRelationshipImporter(imports, propertyReader, buffer);
            }
        }
    }
}
