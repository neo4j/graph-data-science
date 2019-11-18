/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.proc;

import com.google.auto.common.GeneratedAnnotationSpecs;
import com.google.common.collect.Streams;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.NameAllocator;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.annotation.ValueClass;

import javax.annotation.processing.Messager;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.auto.common.MoreTypes.asTypeElement;
import static com.google.auto.common.MoreTypes.isTypeOf;

final class GenerateConfiguration {

    private static final String CONFIG_VAR = "config";

    private final Messager messager;
    private final Elements elementUtils;
    private final SourceVersion sourceVersion;
    private final TypeMirror mapWrapperType;

    GenerateConfiguration(Messager messager, Elements elementUtils, SourceVersion sourceVersion) {
        this.messager = messager;
        this.elementUtils = elementUtils;
        this.sourceVersion = sourceVersion;
        mapWrapperType = elementUtils.getTypeElement("org.neo4j.graphalgo.core.CypherMapWrapper").asType();
    }

    JavaFile generateConfig(ConfigParser.Spec config, String className) {
        PackageElement rootPackage = elementUtils.getPackageOf(config.root());
        String packageName = rootPackage.getQualifiedName().toString();
        TypeSpec typeSpec = process(config, packageName, className);
        return JavaFile.builder(packageName, typeSpec).build();
    }

    private TypeSpec process(ConfigParser.Spec config, String packageName, String generatedClassName) {
        FieldDefinitions fieldDefinitions = defineFields(config);
        return classBuilder(config, packageName, generatedClassName)
            .addFields(fieldDefinitions.fields())
            .addMethod(defineConstructor(config, fieldDefinitions.names()))
            .addMethods(defineGetters(config, fieldDefinitions.names()))
            .build();
    }

    private TypeSpec.Builder classBuilder(ConfigParser.Spec config, String packageName, String generatedClassName) {
        TypeSpec.Builder classBuilder = createNewClass(config, packageName, generatedClassName);
        inheritFrom(classBuilder, config);
        addGeneratedAnnotation(classBuilder);
        return classBuilder;
    }

    private TypeSpec.Builder createNewClass(ConfigParser.Spec config, String packageName, String generatedClassName) {
        return TypeSpec
            .classBuilder(ClassName.get(packageName, generatedClassName))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addOriginatingElement(config.root());
    }

    private void inheritFrom(TypeSpec.Builder classBuilder, ConfigParser.Spec config) {
        TypeName parentName = TypeName.get(config.rootType());
        if (config.rootIsInterface()) {
            classBuilder.addSuperinterface(parentName);
        } else {
            classBuilder.superclass(parentName);
        }
    }

    private void addGeneratedAnnotation(TypeSpec.Builder classBuilder) {
        GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            ConfigurationProcessor.class
        ).ifPresent(classBuilder::addAnnotation);
    }

    private FieldDefinitions defineFields(ConfigParser.Spec config) {
        NameAllocator names = new NameAllocator();
        ImmutableFieldDefinitions.Builder builder = ImmutableFieldDefinitions.builder().names(names);
        config.members().stream().map(member ->
            FieldSpec.builder(
                TypeName.get(member.method().getReturnType()),
                names.newName(member.name(), member),
                Modifier.PRIVATE, Modifier.FINAL
            ).build()
        ).forEach(builder::addField);
        return builder.build();
    }

    private MethodSpec defineConstructor(ConfigParser.Spec config, NameAllocator names) {
        MethodSpec.Builder code = MethodSpec
            .constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(
                TypeName.get(mapWrapperType),
                names.newName(CONFIG_VAR, CONFIG_VAR)
            );

        config.members().stream()
            .map(member -> codeForMemberDefinition(names, member))
            .flatMap(Streams::stream)
            .map(this::codeForMemberDefinition)
            .forEach(code::addStatement);

        return code.build();
    }

    private Optional<MemberDefinition> codeForMemberDefinition(NameAllocator names, ConfigParser.Member member) {
        ImmutableMemberDefinition.Builder builder = ImmutableMemberDefinition
            .builder()
            .fieldName(names.get(member))
            .configParamName(names.get(CONFIG_VAR))
            .configKey(member.name())
            .methodPrefix("require");

        TypeMirror returnType = member.method().getReturnType();
        switch (returnType.getKind()) {
            case BOOLEAN:
                builder.methodName("Bool");
                break;
            case INT:
                builder.methodName("Int");
                break;
            case LONG:
                builder.methodName("Long");
                break;
            case DOUBLE:
                builder.methodName("Double");
                break;
            case BYTE:
            case SHORT:
            case FLOAT:
                builder
                    .methodName("Number")
                    .converter(c -> CodeBlock.of("$L.$LValue()", c, returnType));
                break;
            case DECLARED:
                if (isTypeOf(String.class, returnType)) {
                    builder.methodName("String");
                } else {
                    builder
                        .methodName("Checked")
                        .expectedType(CodeBlock.of("$T.class", ClassName.get(asTypeElement(returnType))));
                }
                break;
            default:
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Usupported return type: " + returnType,
                    member.method()
                );
                return Optional.empty();
        }

        if (member.method().isDefault()) {
            builder
                .methodPrefix("get")
                .defaultProvider(CodeBlock.of(
                    "$T.super.$N()",
                    member.owner().asType(),
                    member.name()
                ));
        }

        return Optional.of(builder.build());
    }

    private CodeBlock codeForMemberDefinition(MemberDefinition definition) {
        CodeBlock.Builder code = CodeBlock.builder().add(
            "this.$N = $N.$L$L($S",
            definition.fieldName(),
            definition.configParamName(),
            definition.methodPrefix(),
            definition.methodName(),
            definition.configKey()
        );
        definition.defaultProvider().ifPresent(d -> code.add(", $L", d));
        definition.expectedType().ifPresent(t -> code.add(", $L", t));
        CodeBlock codeBlock = code.add(")").build();
        return definition.converter()
            .map(c -> c.apply(codeBlock))
            .orElse(codeBlock);
    }

    private Iterable<MethodSpec> defineGetters(ConfigParser.Spec config, NameAllocator names) {
        return config.members().stream().map(member -> MethodSpec
            .overriding(member.method())
            .addStatement("return this.$N", names.get(member))
            .build()).collect(Collectors.toList());
    }

    @ValueClass
    interface FieldDefinitions {
        List<FieldSpec> fields();

        NameAllocator names();
    }

    @ValueClass
    interface MemberDefinition {
        String fieldName();

        String configParamName();

        String methodPrefix();

        String methodName();

        String configKey();

        Optional<CodeBlock> defaultProvider();

        Optional<CodeBlock> expectedType();

        Optional<UnaryOperator<CodeBlock>> converter();
    }
}
