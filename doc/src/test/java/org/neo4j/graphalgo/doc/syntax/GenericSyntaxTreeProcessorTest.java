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
package org.neo4j.graphalgo.doc.syntax;

import org.asciidoctor.Asciidoctor;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SoftAssertionsExtension.class)
class GenericSyntaxTreeProcessorTest {

    @Test
    void testGenericSyntaxTreeProcessor(SoftAssertions softAssertions) throws URISyntaxException {
        var asciidoctor = Asciidoctor.Factory.create();
        asciidoctor.javaExtensionRegistry()
            .treeprocessor(
                new GenericSyntaxTreeProcessor(
                    Map.of(GenericSyntaxTreeProcessor.SyntaxMode.STREAM, LouvainStreamProc.StreamResult.class),
                    softAssertions
                ));

        var file = Paths.get(getClass().getClassLoader().getResource("include-with-syntax.adoc").toURI()).toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Collections.emptyMap());

        softAssertions.assertAll();

    }

}
