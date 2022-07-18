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
package org.neo4j.gds.core.utils;

import org.apache.commons.io.output.WriterOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.compat.OutputStreamLogBuilder;
import org.neo4j.gds.graphbuilder.GraphBuilder;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressLoggingTest extends BaseTest {

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    @BeforeEach
    void setup() {
        GraphBuilder.create(db)
            .setLabel(LABEL)
            .setRelationship(RELATIONSHIP)
            .newGridBuilder()
            .createGrid(100, 10)
            .forEachRelInTx(rel -> {
                rel.setProperty(PROPERTY, Math.random() * 5); // (0-5)
            })
            .close();
    }

    @Test
    void testLoad() {
        final StringWriter buffer = new StringWriter();

        new StoreLoaderBuilder()
            .databaseService(db)
            .log(testLogger(buffer))
            .addNodeLabel(LABEL)
            .addRelationshipType(RELATIONSHIP)
            .addRelationshipProperty(PropertyMapping.of(PROPERTY, 1.0))
            .build()
            .graph();

        final String output = buffer.toString();

        assertThat(output)
            .isNotEmpty()
            .contains("Loading");
    }

    private static Log testLogger(StringWriter writer) {
        var outStream = new WriterOutputStream(writer, StandardCharsets.UTF_8);
        return new OutputStreamLogBuilder(outStream).level(Level.DEBUG).category("Test").build();
    }
}
