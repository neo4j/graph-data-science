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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static javax.tools.StandardLocation.CLASS_OUTPUT;

/**
 * An annotation processor that creates files to enable service loading for procedures, user functions and aggregations.
 * <p>
 * Only things listed in the session allow list will be written (and thus loaded).
 */
@AutoService(Processor.class)
public class SessionProcedureCollectorProcessor extends BasicAnnotationProcessor {

    private final Set<TypeElement> proceduresWithAnnotations = new HashSet<>();
    private final Set<TypeElement> functionsWithAnnotations = new HashSet<>();
    private final Set<TypeElement> aggregationsWithAnnotations = new HashSet<>();

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_17;
    }

    @Override
    protected Iterable<? extends Step> steps() {
        return List.of(new SessionProcedureCollectorStep(
            proceduresWithAnnotations,
            functionsWithAnnotations,
            aggregationsWithAnnotations
        ));
    }

    @Override
    protected void postRound(RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            tryWriteElements();
        }
    }

    private void tryWriteElements() {
        try {
            writeElements();
            proceduresWithAnnotations.clear();
        } catch (IOException e) {
            logError(e,
                String.format(
                    Locale.ENGLISH,
                    "Failed to write procedures for service loading. First: %s",
                    proceduresWithAnnotations
                )
            );
        }
    }

    private void writeElements() throws IOException {
        if (!proceduresWithAnnotations.isEmpty()) {
            writeElementsOfType(SessionProcedureCollectorStep.PROCEDURE, proceduresWithAnnotations);
        }
        if (!functionsWithAnnotations.isEmpty()) {
            writeElementsOfType(SessionProcedureCollectorStep.USER_FUNCTION, functionsWithAnnotations);
        }
        if (!aggregationsWithAnnotations.isEmpty()) {
            writeElementsOfType(SessionProcedureCollectorStep.USER_AGGREGATION, aggregationsWithAnnotations);
        }
    }

    private void writeElementsOfType(String typeName, Iterable<TypeElement> elements) throws IOException {
        // we fake being a service so that we get properly merged in the shadow jar
        var path = "META-INF/services/" + typeName;
        var file = processingEnv.getFiler().createResource(CLASS_OUTPUT, "", path);

        try (var writer = new PrintWriter(
            new BufferedOutputStream(file.openOutputStream()),
            true,
            StandardCharsets.UTF_8
        )) {
            for (var element : elements) {
                writer.println(element.getQualifiedName());
            }
        }
    }

    private void logError(Exception e, String message) {
        processingEnv.getMessager().printMessage(
            Diagnostic.Kind.ERROR,
            String.format(
                Locale.ENGLISH,
                message,
                e.getMessage()
            )
        );
    }
}
