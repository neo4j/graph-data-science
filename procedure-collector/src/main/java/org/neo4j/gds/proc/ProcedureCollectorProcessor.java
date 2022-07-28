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
package org.neo4j.gds.proc;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.service.AutoService;

import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static javax.tools.StandardLocation.CLASS_OUTPUT;

@AutoService(Processor.class)
public final class ProcedureCollectorProcessor extends BasicAnnotationProcessor {

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_11;
    }

    private final List<TypeElement> validTypes = new ArrayList<>();

    @Override
    protected Iterable<? extends Step> steps() {
        return List.of(new ProcedureCollector(
            processingEnv.getElementUtils(),
            processingEnv.getMessager(),
            validTypes
        ));
    }

    @Override
    protected void postRound(RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            tryWriteElements();
        }
    }

    private void tryWriteElements() {
        if (validTypes.isEmpty()) {
            return;
        }

        try {
            writeElements();
            validTypes.clear();
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                String.format(
                    Locale.ENGLISH,
                    "Failed to generate callables resource: %s",
                    e.getMessage()
                ),
                validTypes.get(0)
            );
        }
    }

    private void writeElements() throws IOException {
        // we fake being a service so that we get properly merged in the shadow jar
        var path = "META-INF/services/" + ProcedureCollector.GDS_CALLABLE;
        var file = processingEnv.getFiler().createResource(CLASS_OUTPUT, "", path);

        try (var writer = new PrintWriter(
            new BufferedOutputStream(file.openOutputStream()),
            true,
            StandardCharsets.UTF_8
        )) {
            for (var element : validTypes) {
                writer.println(element.getQualifiedName());
            }
        }
    }
}
