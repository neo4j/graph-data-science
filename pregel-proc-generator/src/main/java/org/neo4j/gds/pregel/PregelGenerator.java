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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.pregel.generator.TypeNames;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

class PregelGenerator {

    // produces @Generated meta info
    private final Optional<AnnotationSpec> generatedAnnotationSpec;

    PregelGenerator(Optional<AnnotationSpec> generatedAnnotationSpec) {
        this.generatedAnnotationSpec = generatedAnnotationSpec;
    }

    private Stream<TypeSpec> typesForMode(GDSMode mode, PregelValidation.Spec pregelSpec, SpecificationGenerator specificationGenerator, TypeNames typeNames) {
        var procedure = ProcedureGenerator.forMode(mode, generatedAnnotationSpec, pregelSpec, typeNames)
            .toBuilder()
            .addOriginatingElement(pregelSpec.element())
            .build();
        var specificationBuilder = specificationGenerator.typeSpec(mode)
            .addMethod(specificationGenerator.nameMethod())
            .addMethod(specificationGenerator.algorithmFactoryMethod())
            .addMethod(specificationGenerator.newConfigFunctionMethod())
            .addMethod(specificationGenerator.computationResultConsumerMethod(mode))
            .addOriginatingElement(pregelSpec.element());
        addGeneratedAnnotation(specificationBuilder);
        var specification = specificationBuilder.build();
        return Stream.of(procedure, specification);
    }

    Stream<TypeSpec> generate(PregelValidation.Spec pregelSpec) {
        var typeNames = new TypeNames(
            pregelSpec.rootPackage(),
            pregelSpec.computationName(),
            pregelSpec.configTypeName()
        );
        var algorithmSpec = new AlgorithmGenerator(generatedAnnotationSpec, typeNames)
            .typeSpec()
            .toBuilder()
            .addOriginatingElement(pregelSpec.element())
            .build();
        var algorithmFactorySpec = new AlgorithmFactoryGenerator(generatedAnnotationSpec, typeNames)
            .typeSpec()
            .toBuilder()
            .addOriginatingElement(pregelSpec.element())
            .build();
        var specificationGenerator = new SpecificationGenerator(typeNames);
        return Stream.concat(
            Stream.of(algorithmSpec, algorithmFactorySpec),
            Arrays.stream(pregelSpec.procedureModes())
                .flatMap(mode -> typesForMode(mode, pregelSpec, specificationGenerator, typeNames))
        );
    }

    // produces @Generated meta info
    void addGeneratedAnnotation(TypeSpec.Builder typeSpecBuilder) {
        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);
    }
}
