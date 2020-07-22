/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

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

class PregelGenerator {

    static final String PROCEDURE_SUFFIX = "Proc";
    static final String ALGORITHM_SUFFIX = "Algorithm";

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

        var procedureTypeSpecs = Arrays.stream(pregelSpec.procedureModes())
            .map(mode -> ProcedureGenerator.forMode(mode, elementUtils, sourceVersion, pregelSpec))
            .map(typeSpec -> fileOf(pregelSpec, typeSpec))
            .collect(Collectors.toList());

        procedureTypeSpecs.add(fileOf(pregelSpec, new AlgorithmGenerator(elementUtils, sourceVersion).typeSpec(pregelSpec)));
        return procedureTypeSpecs;
    }

    // produces @Generated meta info
    void addGeneratedAnnotation(TypeSpec.Builder typeSpecBuilder) {
        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);
    }

    ClassName className(PregelValidation.Spec pregelSpec, String suffix) {
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
