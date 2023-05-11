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
package org.neo4j.gds.beta.pregel;

import com.google.auto.common.GeneratedAnnotationSpecs;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.SourceVersion;
import javax.lang.model.util.Elements;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class PregelGenerator {

    static final String PROCEDURE_SUFFIX = "Proc";
    static final String ALGORITHM_SUFFIX = "Algorithm";
    static final String ALGORITHM_FACTORY_SUFFIX = ALGORITHM_SUFFIX + "Factory";
    static final String ALGORITHM_SPECIFICATION_SUFFIX = ALGORITHM_SUFFIX + "Specification";

    private final Elements elementUtils;
    private final SourceVersion sourceVersion;
    // produces @Generated meta info
    private final Optional<AnnotationSpec> generatedAnnotationSpec;

    PregelGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        this.elementUtils = elementUtils;
        this.sourceVersion = sourceVersion;
        this.generatedAnnotationSpec = GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            PregelProcessor.class
        );
    }

    List<JavaFile> generate(PregelValidation.Spec pregelSpec) {
        return Stream.concat(
            Stream.of(
                new AlgorithmGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec(),
                new AlgorithmFactoryGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec()
            ),
            Arrays
                .stream(pregelSpec.procedureModes())
                .map(mode -> ProcedureGenerator.forMode(mode, elementUtils, sourceVersion, pregelSpec))
        )
            .map(typeSpec -> fileOf(pregelSpec, typeSpec))
            .collect(Collectors.toList());
    }

    // produces @Generated meta info
    void addGeneratedAnnotation(TypeSpec.Builder typeSpecBuilder) {
        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);
    }

    ClassName computationClassName(PregelValidation.Spec pregelSpec, String suffix) {
        return ClassName.get(pregelSpec.rootPackage(), pregelSpec.computationName() + suffix);
    }

    private JavaFile fileOf(PregelValidation.Spec pregelSpec, TypeSpec typeSpec) {
        return JavaFile
            .builder(pregelSpec.rootPackage(), typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }
}
