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
package org.neo4j.gds.core.io.file.csv;

import de.siegmar.fastcsv.writer.CsvWriter;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.SingleRowVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class CsvGraphInfoVisitor implements SingleRowVisitor<GraphInfo> {

    public static final String GRAPH_INFO_FILE_NAME = "graph_info.csv";
    public static final String DATABASE_NAME_COLUMN_NAME = "databaseName";
    public static final String NODE_COUNT_COLUMN_NAME = "nodeCount";
    public static final String MAX_ORIGINAL_ID_COLUMN_NAME = "maxOriginalId";
    public static final String REL_TYPE_COUNTS_COLUMN_NAME = "relTypeCounts";
    public static final String INVERSE_INDEXED_REL_TYPES = "inverseIndexedRelTypes";

    private final CsvWriter csvWriter;

    public CsvGraphInfoVisitor(Path fileLocation) {
        try {
            this.csvWriter = CsvWriter
                .builder()
                .build(fileLocation.resolve(GRAPH_INFO_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void export(GraphInfo graphInfo) {
        String inverseIndexedRelTypesString = graphInfo
            .inverseIndexedRelationshipTypes()
            .stream()
            .map(RelationshipType::name)
            .collect(Collectors.joining(";"));

        this.csvWriter.writeRow(
            graphInfo.databaseId().databaseName(),
            Long.toString(graphInfo.nodeCount()),
            Long.toString(graphInfo.maxOriginalId()),
            CsvMapUtil.relationshipCountsToString(graphInfo.relationshipTypeCounts()),
            inverseIndexedRelTypesString
        );
    }

    @Override
    public void close() {
        try {
            this.csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeHeader() {
        this.csvWriter.writeRow(
            DATABASE_NAME_COLUMN_NAME,
            NODE_COUNT_COLUMN_NAME,
            MAX_ORIGINAL_ID_COLUMN_NAME,
            REL_TYPE_COUNTS_COLUMN_NAME,
            INVERSE_INDEXED_REL_TYPES
        );
    }
}
