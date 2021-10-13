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
package org.neo4j.gds.collections.hsa;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.HugeSparseArray;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Set;

public class HugeSparseArrayStep implements BasicAnnotationProcessor.Step {

    private static final Class<HugeSparseArray> HSA_ANNOTATION = HugeSparseArray.class;

    private final Messager messager;
    private final Filer filer;

    private final HugeSparseArrayValidation validation;

    public HugeSparseArrayStep(ProcessingEnvironment processingEnv) {
        this.validation = new HugeSparseArrayValidation(
            processingEnv.getTypeUtils(),
            processingEnv.getElementUtils(),
            processingEnv.getMessager()
        );

        this.messager = processingEnv.getMessager();
        this.filer = processingEnv.getFiler();
    }

    @Override
    public Set<String> annotations() {
        return Set.of(HSA_ANNOTATION.getCanonicalName());
    }

    @Override
    public Set<? extends Element> process(ImmutableSetMultimap<String, Element> elementsByAnnotation) {
        var elements = elementsByAnnotation.get(HSA_ANNOTATION.getCanonicalName());

        ImmutableSet.Builder<Element> elementsToRetry = ImmutableSet.builder();

        for (Element element : elements) {
            if (process(element) == ProcessResult.RETRY) {
                elementsToRetry.add(element);
            }
        }

        return elementsToRetry.build();
    }

    private ProcessResult process(Element element) {
        var validationResult = validation.validate(element);

        if (validationResult.isEmpty()) {
            return ProcessResult.INVALID;
        }

        var spec = validationResult.get();
        var typeSpec = HugeSparseArrayGenerator.generate(spec);
        var javaFile = javaFile(spec.rootPackage().toString(), typeSpec);

        return writeFile(element, javaFile);
    }

    private static JavaFile javaFile(String rootPackage, TypeSpec typeSpec) {
        return JavaFile
            .builder(rootPackage, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private ProcessResult writeFile(Element element, JavaFile file) {
        try {
            file.writeTo(filer);
            return ProcessResult.PROCESSED;
        } catch (IOException e) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Could not write HugeSparseArray java file: " + e.getMessage(),
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
