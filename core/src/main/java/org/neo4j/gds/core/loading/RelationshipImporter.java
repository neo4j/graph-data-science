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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Arrays;
import java.util.Collection;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RelationshipImporter {

    private final AllocationTracker allocationTracker;
    private final AdjacencyBuilder adjacencyBuilder;

    public RelationshipImporter(AllocationTracker allocationTracker, AdjacencyBuilder adjacencyBuilder) {
        this.allocationTracker = allocationTracker;
        this.adjacencyBuilder = adjacencyBuilder;
    }

    public interface Imports {
        long importRelationships(RelationshipsBatchBuffer batches, PropertyReader propertyReader);
    }

    public Imports imports(Orientation orientation, boolean loadProperties) {
        if (orientation == Orientation.UNDIRECTED) {
            return loadProperties
                ? this::importUndirectedWithProperties
                : this::importUndirected;
        } else if (orientation == Orientation.NATURAL) {
            return loadProperties
                ? this::importNaturalWithProperties
                : this::importNatural;
        } else if (orientation == Orientation.REVERSE) {
            return loadProperties
                ? this::importReverseWithProperties
                : this::importReverse;
        } else {
            throw new IllegalArgumentException(formatWithLocale("Unexpected orientation: %s", orientation));
        }
    }

    private long importUndirected(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortBySource();
        int importedOut = importRelationships(buffer, batch, null, adjacencyBuilder);
        batch = buffer.sortByTarget();
        int importedIn = importRelationships(buffer, batch, null, adjacencyBuilder);
        return RawValues.combineIntInt(importedOut + importedIn, 0);
    }

    private long importUndirectedWithProperties(RelationshipsBatchBuffer buffer, PropertyReader reader) {
        int batchLength = buffer.length();
        long[] batch = buffer.sortBySource();
        long[][] outProperties = reader.readProperty(
            buffer.relationshipReferences(),
            buffer.propertyReferences(),
            batchLength / 2,
            adjacencyBuilder.getPropertyKeyIds(),
            adjacencyBuilder.getDefaultValues(),
            adjacencyBuilder.getAggregations(),
            adjacencyBuilder.atLeastOnePropertyToLoad()
        );
        int importedOut = importRelationships(buffer, batch, outProperties, adjacencyBuilder);

        batch = buffer.sortByTarget();
        long[][] inProperties = reader.readProperty(
            buffer.relationshipReferences(),
            buffer.propertyReferences(),
            batchLength / 2,
            adjacencyBuilder.getPropertyKeyIds(),
            adjacencyBuilder.getDefaultValues(),
            adjacencyBuilder.getAggregations(),
            adjacencyBuilder.atLeastOnePropertyToLoad()
        );
        int importedIn = importRelationships(buffer, batch, inProperties, adjacencyBuilder);

        return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);
    }

    private long importNatural(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortBySource();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, adjacencyBuilder), 0);
    }

    private long importNaturalWithProperties(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        int batchLength = buffer.length();
        long[] batch = buffer.sortBySource();
        long[][] outProperties = propertyReader.readProperty(
            buffer.relationshipReferences(),
            buffer.propertyReferences(),
            batchLength / 2,
            adjacencyBuilder.getPropertyKeyIds(),
            adjacencyBuilder.getDefaultValues(),
            adjacencyBuilder.getAggregations(),
            adjacencyBuilder.atLeastOnePropertyToLoad()
        );
        int importedOut = importRelationships(buffer, batch, outProperties, adjacencyBuilder);
        return RawValues.combineIntInt(importedOut, importedOut);
    }

    private long importReverse(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortByTarget();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, adjacencyBuilder), 0);
    }

    private long importReverseWithProperties(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        int batchLength = buffer.length();
        long[] batch = buffer.sortByTarget();
        long[][] inProperties = propertyReader.readProperty(
            buffer.relationshipReferences(),
            buffer.propertyReferences(),
            batchLength / 2,
            adjacencyBuilder.getPropertyKeyIds(),
            adjacencyBuilder.getDefaultValues(),
            adjacencyBuilder.getAggregations(),
            adjacencyBuilder.atLeastOnePropertyToLoad()
        );
        int importedIn = importRelationships(buffer, batch, inProperties, adjacencyBuilder);
        return RawValues.combineIntInt(importedIn, importedIn);
    }

    public interface PropertyReader {
        /**
         * Load the relationship properties for the given batch of relationships.
         * Relationships are represented as two arrays from the {@link RelationshipsBatchBuffer}.
         *
         * @param relationshipReferences   relationship references (IDs)
         * @param propertyReferences       property references (IDs or References)
         * @param numberOfReferences       number of valid entries in the first two arrays
         * @param propertyKeyIds           property key ids to load
         * @param defaultValues            default weight for each property key
         * @param aggregations             the aggregation for each property
         * @param atLeastOnePropertyToLoad true iff there is at least one value in {@code propertyKeyIds} that is not {@link org.neo4j.kernel.api.StatementConstants#NO_SUCH_PROPERTY_KEY} (-1).
         * @return list of property values per relationship property id
         */
        long[][] readProperty(
            long[] relationshipReferences,
            PropertyReference[] propertyReferences,
            int numberOfReferences,
            int[] propertyKeyIds,
            double[] defaultValues,
            Aggregation[] aggregations,
            boolean atLeastOnePropertyToLoad
        );
    }

    Collection<Runnable> flushTasks() {
        return adjacencyBuilder.flushTasks();
    }

    PropertyReader storeBackedPropertiesReader(KernelTransaction kernelTransaction) {
        return (relationshipReferences, propertyReferences, numberOfReferences, relationshipProperties, defaultPropertyValues, aggregations, atLeastOnePropertyToLoad) -> {
            long[][] properties = new long[relationshipProperties.length][numberOfReferences];
            if (atLeastOnePropertyToLoad) {
                try (PropertyCursor pc = Neo4jProxy.allocatePropertyCursor(kernelTransaction)) {
                    double[] relProps = new double[relationshipProperties.length];
                    for (int i = 0; i < numberOfReferences; i++) {
                        Neo4jProxy.relationshipProperties(
                            kernelTransaction,
                            relationshipReferences[i],
                            propertyReferences[i],
                            pc
                        );
                        ReadHelper.readProperties(
                            pc,
                            relationshipProperties,
                            defaultPropertyValues,
                            aggregations,
                            relProps
                        );
                        for (int j = 0; j < relProps.length; j++) {
                            properties[j][i] = Double.doubleToLongBits(relProps[j]);
                        }
                    }
                }
            } else {
                for (int i = 0; i < numberOfReferences; i++) {
                    for (int j = 0; j < defaultPropertyValues.length; j++) {
                        double value = aggregations[j].normalizePropertyValue(defaultPropertyValues[j]);
                        properties[j][i] = Double.doubleToLongBits(value);
                    }
                }
            }
            return properties;
        };
    }


    public static PropertyReader preLoadedPropertyReader() {
        return (relationshipReferences, propertyReferences, numberOfReferences, weightProperty, defaultWeight, aggregations, atLeastOnePropertyToLoad) -> {
            long[] properties = Arrays.copyOf(relationshipReferences, numberOfReferences);
            return new long[][]{properties};
        };
    }

    private int importRelationships(
        RelationshipsBatchBuffer buffer,
        long[] batch,
        long[][] properties,
        AdjacencyBuilder adjacency
    ) {
        int batchLength = buffer.length();

        int[] offsets = buffer.spareInts();
        long[] targets = buffer.spareLongs();

        long source, target, prevSource = batch[0];
        int offset = 0, nodesLength = 0;

        for (int i = 0; i < batchLength; i += 2) {
            source = batch[i];
            target = batch[1 + i];
            // If rels come in chunks for same source node
            // then we can skip adding an extra offset
            if (source > prevSource) {
                offsets[nodesLength++] = offset;
                prevSource = source;
            }
            targets[offset++] = target;
        }
        offsets[nodesLength++] = offset;

        adjacency.addAll(
            batch,
            targets,
            properties,
            offsets,
            nodesLength,
            allocationTracker
        );

        return batchLength >> 1; // divide by 2
    }
}
