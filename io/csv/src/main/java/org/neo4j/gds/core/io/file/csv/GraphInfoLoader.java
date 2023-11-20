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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ImmutableDatabaseInfo;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.ImmutableGraphInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class GraphInfoLoader {
    private final Path graphInfoPath;
    private final ObjectReader objectReader;

    public GraphInfoLoader(Path csvDirectory, CsvMapper csvMapper) {
        this.graphInfoPath = csvDirectory.resolve(CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME);

        csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        objectReader = csvMapper.readerFor(GraphInfoLine.class).with(schema);
    }

    public GraphInfo load() {
        try (var fileReader = Files.newBufferedReader(graphInfoPath, StandardCharsets.UTF_8)) {
            var mappingIterator = objectReader.<GraphInfoLine>readValues(fileReader);

            var line = mappingIterator.next();
            var databaseId = DatabaseId.of(line.databaseName);

            // This is for backwards compatibility. If the remote database id is not specified
            // we use the default neo4j database instead for remote database locations.
            var remoteDatabaseId = Optional.ofNullable(StringUtils.trimToNull(line.remoteDatabaseId));
            if (line.databaseLocation == DatabaseLocation.REMOTE && remoteDatabaseId.isEmpty()) {
                remoteDatabaseId = Optional.of(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
            }
            var databaseInfo = ImmutableDatabaseInfo.builder()
                .databaseId(databaseId)
                .databaseLocation(line.databaseLocation)
                .remoteDatabaseId(remoteDatabaseId.map(DatabaseId::of))
                .build();
            return ImmutableGraphInfo.builder()
                .databaseInfo(databaseInfo)
                .idMapBuilderType(line.idMapBuilderType)
                .nodeCount(line.nodeCount)
                .maxOriginalId(line.maxOriginalId)
                .relationshipTypeCounts(line.relTypeCounts)
                .inverseIndexedRelationshipTypes(line.inverseIndexedRelTypes)
                .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class GraphInfoLine {
        // Can parse a databaseId if present, for backwards compatibility. The property is never used.
        @JsonIgnore
        @JsonProperty
        UUID databaseId;

        @JsonProperty
        String databaseName;

        @JsonProperty
        DatabaseLocation databaseLocation = DatabaseLocation.LOCAL;

        @JsonProperty
        String remoteDatabaseId = null;

        @JsonProperty
        String idMapBuilderType = IdMap.NO_TYPE;

        @JsonProperty
        long nodeCount;

        @JsonProperty
        long maxOriginalId;

        @JsonDeserialize(using = RelationshipTypesDeserializer.class)
        Map<RelationshipType, Long> relTypeCounts = Map.of();

        @JsonDeserialize(using = InverseIndexedRelTypesDeserializer.class)
        List<RelationshipType> inverseIndexedRelTypes = List.of();
    }

    static class RelationshipTypesDeserializer extends StdDeserializer<Map<RelationshipType, Long>> {

        RelationshipTypesDeserializer() {
            this(null);
        }

        RelationshipTypesDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public Map<RelationshipType, Long> deserialize(
            JsonParser parser,
            DeserializationContext ctxt
        ) throws IOException {
            String mapString = parser.getText();
            return CsvMapUtil.fromString(mapString, RelationshipType::of, Long::parseLong);
        }
    }

    static class InverseIndexedRelTypesDeserializer extends StdDeserializer<List<RelationshipType>> {

        InverseIndexedRelTypesDeserializer() {
            this(null);
        }

        InverseIndexedRelTypesDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public List<RelationshipType> deserialize(
            JsonParser parser,
            DeserializationContext ctxt
        ) throws IOException {
            return Arrays.stream(parser.getText().split(";"))
                .filter(s -> !s.isEmpty())
                .map(RelationshipType::of)
                .collect(Collectors.toList());
        }
    }
}
