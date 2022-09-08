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
package org.neo4j.gds.doc;

import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.IncludeProcessor;
import org.asciidoctor.extension.PreprocessorReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public final class PartialsIncludeProcessor extends IncludeProcessor {
    @Override
    public boolean handles(String target) {
        return target.contains("partial$");
    }

    @Override
    public void process(Document document, PreprocessorReader reader, String target, Map<String, Object> attributes) {
        Path partial = resolvePartial(target);
        String partialText = readPartial(partial);

        reader.push_include(partialText, target, null, Integer.MIN_VALUE, Collections.emptyMap());
    }

    @NotNull
    private Path resolvePartial(String target) {
        var relativePathToPartialFile = target.replace("partial$", "partials");
        return DocumentationTestToolsConstants.ASCIIDOC_PATH.resolve(relativePathToPartialFile);
    }

    private String readPartial(Path partial) {
        try {
            return Files.readString(partial);
        } catch (IOException e) {
            throw new IllegalStateException("Error reading '" + partial + "'", e);
        }
    }
}
