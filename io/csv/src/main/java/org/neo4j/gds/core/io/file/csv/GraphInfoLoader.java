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
import org.apache.commons.lang3.StringUtils;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.core.io.file.GraphInfo;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvReadFeature;
import tools.jackson.dataformat.csv.CsvSchema;

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

public class GraphInfoLoader {
    private final Path graphInfoPath;
    private final ObjectReader objectReader;

    public GraphInfoLoader(Path csvDirectory, CsvMapper csvMapper) {
        this.graphInfoPath = csvDirectory.resolve(CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME);
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        this.objectReader = csvMapper.rebuild()
            .enable(CsvReadFeature.TRIM_SPACES)
            .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .build()
            .readerFor(GraphInfoLine.class)
            .with(schema);
    }

    public GraphInfo load() {
        try (var fileReader = Files.newBufferedReader(graphInfoPath, StandardCharsets.UTF_8)) {
            var mappingIterator = objectReader.<GraphInfoLine>readValues(fileReader);

            var line = mappingIterator.next();

            var databaseId = DatabaseId.of(line.databaseName);
            var remoteDatabaseId = Optional.ofNullable(StringUtils.trimToNull(line.remoteDatabaseId)).map(DatabaseId::of);

            var databaseInfo = DatabaseInfo.create(
                databaseId,
                line.databaseLocation,
                remoteDatabaseId
            );
            return GraphInfo.builder()
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
            this(Map.class);
        }

        RelationshipTypesDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public Map<RelationshipType, Long> deserialize(JsonParser parser, DeserializationContext ctxt) {
            String mapString = parser.getString();
            return CsvMapUtil.fromString(mapString, RelationshipType::of, Long::parseLong);
        }
    }

    static class InverseIndexedRelTypesDeserializer extends StdDeserializer<List<RelationshipType>> {

        InverseIndexedRelTypesDeserializer() {
            this(List.class);
        }

        InverseIndexedRelTypesDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public List<RelationshipType> deserialize(JsonParser parser, DeserializationContext ctxt) {
            return Arrays.stream(parser.getString().split(";"))
                .filter(s -> !s.isEmpty())
                .map(RelationshipType::of)
                .toList();
        }
    }
}
