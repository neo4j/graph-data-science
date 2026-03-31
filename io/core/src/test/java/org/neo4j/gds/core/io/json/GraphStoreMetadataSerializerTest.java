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
package org.neo4j.gds.core.io.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.gds.core.io.json.Utils.deserialize;
import static org.neo4j.gds.core.io.json.Utils.formatWithoutWhitespace;
import static org.neo4j.gds.core.io.json.Utils.serialize;

class GraphStoreMetadataSerializerTest {

    @Test
    void serializeDatabaseInfo() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());

        var result = serialize(databaseInfo);

        var expected = formatWithoutWhitespace(
            """
                {
                     "databaseName":"%s",
                     "databaseLocation":"%s",
                     "remoteDatabaseId":null
                }
                """, databaseName, databaseLocation
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void serializeDatabaseInfoWithRemote() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.REMOTE;
        var remoteDatabaseId = "foo";
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.of(remoteDatabaseId));

        var result = serialize(databaseInfo);

        var expected = formatWithoutWhitespace(
            """
                {
                     "databaseName":"%s",
                     "databaseLocation":"%s",
                     "remoteDatabaseId":"%s"
                }
                """, databaseName, databaseLocation, remoteDatabaseId
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void roundTripDatabaseInfo() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());

        var result = deserialize(serialize(databaseInfo), DatabaseInfo.class);

        assertThat(result).isEqualTo(databaseInfo);
    }

    @Test
    void serializeIdMapInfo() throws JsonProcessingException {
        var idMapType = ArrayIdMapBuilder.ID;
        long nodeCount = 1337L;
        long maxOriginalId = 1984L;
        var nodeLabelCounts = new TreeMap<String, Long>();
        nodeLabelCounts.put("A", 42L);
        nodeLabelCounts.put("B", 1984L);

        var idMapInfo = new IdMapInfo(idMapType, nodeCount, maxOriginalId, nodeLabelCounts);

        var result = serialize(idMapInfo);

        var expected = formatWithoutWhitespace(
            """
                {
                    "idMapType": "%s",
                    "nodeCount": %d,
                    "maxOriginalId": %d,
                    "nodeLabelCounts": {
                        "A": 42,
                        "B": 1984
                    }
                }
                """, idMapType, nodeCount, maxOriginalId
        );

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void roundTripIdMapInfo() throws JsonProcessingException {
        var idMapType = ArrayIdMapBuilder.ID;
        long nodeCount = 1337L;
        long maxOriginalId = 1984L;
        var nodeLabelCounts = new TreeMap<String, Long>();
        nodeLabelCounts.put("A", 42L);
        nodeLabelCounts.put("B", 1984L);

        var idMapInfo = new IdMapInfo(idMapType, nodeCount, maxOriginalId, nodeLabelCounts);

        var result = deserialize(serialize(idMapInfo), IdMapInfo.class);

        assertThat(result).isEqualTo(idMapInfo);
    }

    @Test
    void serializeDefaultValue() throws JsonProcessingException {
        var defaultValue = org.neo4j.gds.api.DefaultValue.of(42L, true);
        var input = new DefaultValue(defaultValue.getObject(), defaultValue.isUserDefined());

        var result = serialize(input);

        System.out.println(result);
    }

    @Test
    void serializeGraphStoreMetadata() throws JsonProcessingException {
        var graphStoreMetadata = getGraphStoreMetadata();

        var result = serialize(graphStoreMetadata);

        var expected = formatWithoutWhitespace(
            """
                {
                  "databaseInfo": {
                    "databaseName": "neo",
                    "databaseLocation": "LOCAL",
                    "remoteDatabaseId": null
                  },
                  "writeMode": "LOCAL",
                  "idMapInfo": {
                    "idMapType": "array",
                    "nodeCount": 42,
                    "maxOriginalId": 42,
                    "nodeLabelCounts": {
                      "A": 42
                    }
                  },
                  "relationshipInfo": {
                    "REL": {
                      "relationshipCount": 1337,
                      "isInverseIndexed": false,
                      "propertyCount": 1
                    }
                  },
                  "nodeSchema": {
                    "A": {
                      "propertySchemas": {
                        "foo": {
                          "valueType": "LONG",
                          "defaultValue": {
                            "value": {
                              "long": 42
                            },
                            "isUserDefined": false
                          },
                          "propertyState": "PERSISTENT"
                        }
                      }
                    }
                  },
                  "relationshipSchema": {
                    "REL": {
                      "direction": "DIRECTED",
                      "propertySchemas": {
                        "bar": {
                          "valueType": "DOUBLE",
                          "defaultValue": {
                            "value": 4.2,
                            "isUserDefined": false
                          },
                          "propertyState": "PERSISTENT",
                          "aggregation": "NONE"
                        }
                      }
                    }
                  }
                }
                """
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void roundTripGraphStoreMetadata() throws JsonProcessingException {
        var graphStoreMetadata = getGraphStoreMetadata();

        var result = deserialize(serialize(graphStoreMetadata), GraphStoreMetadata.class);

        assertThat(result).isEqualTo(graphStoreMetadata);
    }

    private static @NonNull GraphStoreMetadata getGraphStoreMetadata() {
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo("neo", databaseLocation, Optional.empty());
        var writeMode = WriteMode.LOCAL;
        var idMapInfo = new IdMapInfo(ArrayIdMapBuilder.ID, 42, 42, Map.of("A", 42L));
        var relationshipInfo = Map.of(
            "REL", new RelationshipInfo(1337L, false, 1)
        );
        var nodeSchema = Map.of(
            "A", new NodeSchema(Map.of(
                "foo", new NodePropertySchema(ValueType.LONG, new DefaultValue(42L, false), PropertyState.PERSISTENT)

            ))
        );
        var relationshipSchema = Map.of(
            "REL", new RelationshipSchema(
                Direction.DIRECTED,
                Map.of(
                    "bar",
                    new RelationshipPropertySchema(
                        ValueType.DOUBLE,
                        new DefaultValue(4.2, false),
                        PropertyState.PERSISTENT,
                        Aggregation.NONE
                    )
                )
            )
        );
        return new GraphStoreMetadata(
            databaseInfo,
            writeMode,
            idMapInfo,
            relationshipInfo,
            nodeSchema,
            relationshipSchema
        );
    }
}
