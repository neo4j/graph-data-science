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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SoftAssertionsExtension.class)
abstract class SyntaxTestBase {
    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");

    final Asciidoctor asciidoctor = create();

    @Test
    void runSyntaxTest(SoftAssertions softAssertions) {
        asciidoctor.javaExtensionRegistry().treeprocessor(syntaxTreeProcessor(softAssertions));

        var docFile = ASCIIDOC_PATH.resolve(adocFile()).toFile();
        assertThat(docFile).exists().canRead();
        asciidoctor.loadFile(docFile, Collections.emptyMap());

        softAssertions.assertAll();
    }

    abstract Iterable<SyntaxMode> syntaxModes();

    abstract String adocFile();

    private ProcedureSyntaxAutoChecker syntaxTreeProcessor(SoftAssertions syntaxAssertions) {
        return new ProcedureSyntaxAutoChecker(syntaxModes(), syntaxAssertions);
    }
}
