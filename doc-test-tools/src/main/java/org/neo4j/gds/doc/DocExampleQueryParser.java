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

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.neo4j.gds.doc.syntax.DocQuery;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class DocExampleQueryParser {

    private final QueryCollectingTreeProcessor queryProcessor;
    private final PartialsIncludeProcessor includeProcessor;
    private final File workingDirectory;
    private final List<String> adocFilePaths;

    public DocExampleQueryParser(File workingDirectory, List<String> adocFilePaths) {
        this.workingDirectory = workingDirectory;
        this.adocFilePaths = adocFilePaths;
        this.queryProcessor = new QueryCollectingTreeProcessor();
        this.includeProcessor = new PartialsIncludeProcessor();
    }

    public ParseResult parseAndCollect() {
        try (var asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaExtensionRegistry()
                .includeProcessor(includeProcessor)
                .treeprocessor(queryProcessor);

            var options = Options.builder()
                .toDir(workingDirectory) // Make sure we don't write anything in the project.
                .safe(SafeMode.UNSAFE); // By default, we are forced to use relative path which we don't want.

            for (var docFile : adocFiles()) {
                asciidoctor.convertFile(docFile, options.build());
            }
        }

        return new ParseResult(
            queryProcessor.getBeforeEachQueries(),
            queryProcessor.getBeforeAllQueries(),
            queryProcessor.getQueryExampleGroups()
        );
    }

    private List<File> adocFiles() {
        return adocFilePaths
            .stream()
            .map(DocumentationTestToolsConstants.ASCIIDOC_PATH::resolve)
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    public record ParseResult(
            List<DocQuery> beforeEachQueries,
            List<DocQuery> beforeAllQueries,
            List<QueryExampleGroup> queryExampleGroups) {
    }
}
