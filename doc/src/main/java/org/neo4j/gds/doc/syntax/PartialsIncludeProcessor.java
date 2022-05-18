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
package org.neo4j.gds.doc.syntax;

import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.IncludeProcessor;
import org.asciidoctor.extension.PreprocessorReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

final class PartialsIncludeProcessor extends IncludeProcessor {

    @Override
    public boolean handles(String target) {
        return target.contains("partial$");
    }

    @Override
    public void process(Document document, PreprocessorReader reader, String target, Map<String, Object> attributes) {
        var base_dir = document.getOptions().get("base_dir").toString();
        var relativePathToPartialFile = target.replace("partial$", "partials");
        var partialFile = Paths.get(base_dir, relativePathToPartialFile).toFile();
        try(var partialFileReader = new BufferedReader(new FileReader(partialFile, StandardCharsets.UTF_8))) {
            var partialFileContent = partialFileReader
                .lines()
                .collect(Collectors.joining(System.lineSeparator()));

            reader.push_include(
                partialFileContent,
                target,
                new File(".").getAbsolutePath(),
                1,
                attributes
            );

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
