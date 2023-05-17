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
package org.neo4j.gds.pregel;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class PregelProcessorStep implements BasicAnnotationProcessor.Step {

    private static final Class<PregelProcedure> ANNOTATION_CLASS = PregelProcedure.class;

    private final Messager messager;
    private final Filer filer;
    private final PregelValidation pregelValidation;
    private final PregelGenerator pregelGenerator;

    PregelProcessorStep(
        Messager messager,
        Filer filer,
        PregelValidation pregelValidation,
        PregelGenerator pregelGenerator
    ) {
        this.messager = messager;
        this.filer = filer;
        this.pregelValidation = pregelValidation;
        this.pregelGenerator = pregelGenerator;
    }

    @Override
    public Set<String> annotations() {
        return Set.of(ANNOTATION_CLASS.getCanonicalName());
    }

    @Override
    public Set<? extends Element> process(ImmutableSetMultimap<String, Element> elementsByAnnotation) {
        Set<Element> elements = elementsByAnnotation.get(ANNOTATION_CLASS.getCanonicalName());
        ImmutableSet.Builder<Element> elementsToRetry = ImmutableSet.builder();

        for (Element element : elements) {
            ProcessResult result = process(element);
            if (result == ProcessResult.RETRY) {
                elementsToRetry.add(element);
            }
        }
        return elementsToRetry.build();
    }

    private ProcessResult process(Element element) {
        var maybePregelSpec = pregelValidation.validate(element);

        return maybePregelSpec.map((pregelSpec) -> {
            var files = pregelGenerator.generate(pregelSpec)
                .map(typeSpec -> fileOf(pregelSpec, typeSpec))
                .collect(Collectors.toList());
            return writeFiles(element, files);
        }).orElse(ProcessResult.INVALID);
    }

    private JavaFile fileOf(PregelValidation.Spec pregelSpec, TypeSpec typeSpec) {
        return JavaFile
            .builder(pregelSpec.rootPackage(), typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private ProcessResult writeFiles(Element element, List<JavaFile> files) {
        try {
            for (JavaFile file : files) {
                file.writeTo(filer);
            }
            return ProcessResult.PROCESSED;
        } catch (IOException e) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Could not write Pregel java file: " + e.getMessage(),
                element
            );
            return ProcessResult.RETRY;
        }
    }

    enum ProcessResult {
        PROCESSED,
        INVALID,
        RETRY
    }
}
