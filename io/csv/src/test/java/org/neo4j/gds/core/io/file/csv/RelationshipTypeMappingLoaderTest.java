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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.io.file.csv.CsvRelationshipTypeMappingVisitor.TYPE_MAPPING_FILE_NAME;

class RelationshipTypeMappingLoaderTest {

    @TempDir
    Path exportDir;

    @Test
    void shouldLoadMappings() throws IOException {
        FileUtils.write(
            exportDir.resolve(TYPE_MAPPING_FILE_NAME).toFile(),
            """
                index,type
                type1,FOO
                type2,BAR""",
            Charset.defaultCharset()
        );

        var maybeMapping = new RelationshipTypeMappingLoader(exportDir).load();
        assertThat(maybeMapping.isPresent()).isTrue();
        var mapping = maybeMapping.get();
        assertThat(mapping).containsExactlyInAnyOrderEntriesOf(Map.of(
            "type1", "FOO",
            "type2", "BAR"));
    }
}
