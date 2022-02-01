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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.core.CypherMapWrapper;

import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreTypes.asTypeElement;
import static com.google.auto.common.MoreTypes.isTypeOf;

final class GenerateConfigurationBuilder {

    private GenerateConfigurationBuilder() {}

    static TypeSpec defineConfigBuilder(
        TypeName configInterfaceType,
        List<GenerateConfiguration.MemberDefinition> configImplMembers,
        ClassName builderClassName,
        String generatedClassName,
        List<ParameterSpec> constructorParameters,
        String configMapParameterName,
        Optional<MethodSpec> maybeFactoryFunction
    ) {
        TypeSpec.Builder configBuilderClass = TypeSpec.classBuilder("Builder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        configBuilderClass.addField(
            ParameterizedTypeName.get(Map.class, String.class, Object.class),
            configMapParameterName,
            Modifier.FINAL,
            Modifier.PRIVATE
        );

        configBuilderClass.addMethod(MethodSpec.constructorBuilder()
            .addStatement("this.$N = new $T<>()", configMapParameterName, HashMap.class)
            .addModifiers(Modifier.PUBLIC).build()
        );

        constructorParameters.stream()
            .filter(p -> !p.name.equals(configMapParameterName))
            .forEach(parameter -> configBuilderClass.addField(parameter.type, parameter.name, Modifier.PRIVATE));

        return configBuilderClass
            .addMethods(defineConfigParameterSetters(configImplMembers, builderClassName))
            .addMethods(defineConfigMapEntrySetters(configImplMembers, configMapParameterName, builderClassName))
            .addMethod(defineBuildMethod(
                configInterfaceType,
                generatedClassName,
                constructorParameters,
                configMapParameterName,
                maybeFactoryFunction
            ))
            .build();
    }

    private static List<MethodSpec> defineConfigParameterSetters(
        List<GenerateConfiguration.MemberDefinition> implMembers,
        ClassName builderClassName
    ) {
        // if member -> get actual type by checking whatever the convert with method has
        return implMembers.stream()
            .filter(implMember -> implMember.member().isConfigParameter())
            .map(implMember -> MethodSpec.methodBuilder(implMember.member().methodName())
                .addModifiers(Modifier.PUBLIC)
                .addParameter(TypeName.get(implMember.parameterType()), implMember.member().methodName())
                .returns(builderClassName)
                .addCode(CodeBlock.builder()
                    .addStatement("this.$1N = $1N", implMember.member().methodName())
                    .addStatement("return this")
                    .build()
                ).build()
            )
            .collect(Collectors.toList());
    }

    private static List<MethodSpec> defineConfigMapEntrySetters(
        List<GenerateConfiguration.MemberDefinition> implMembers,
        String builderConfigMapFieldName,
        ClassName builderClassName
    ) {
        return implMembers.stream()
            .filter(implMember -> implMember.member().isConfigMapEntry())
            .flatMap(implMember -> {
                var setterMethods = Stream.<MethodSpec>builder();
                String configKeyName = implMember.member().methodName();

                MethodSpec.Builder setMethodBuilder = MethodSpec.methodBuilder(configKeyName)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(unpackedType(implMember.parameterType()), configKeyName)
                    .returns(builderClassName)
                    .addCode(CodeBlock.builder()
                        .addStatement(
                            "this.$N.put(\"$L\", $N)",
                            builderConfigMapFieldName,
                            implMember.member().lookupKey(),
                            configKeyName
                        )
                        .addStatement("return this")
                        .build()
                    );

                setterMethods.add(setMethodBuilder.build());

                if (isTypeOf(Optional.class, implMember.parameterType())) {
                    String lambdaVarName = "actual" + configKeyName;

                    var optionalSetterBuilder = MethodSpec.methodBuilder(configKeyName)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(TypeName.get(implMember.parameterType()), configKeyName)
                        .returns(builderClassName)
                        .addStatement(
                            "$1N.ifPresent($2N -> this.$3N.put(\"$4L\", $2N))",
                            configKeyName,
                            lambdaVarName,
                            builderConfigMapFieldName,
                            implMember.member().lookupKey()
                        ).addStatement("return this");

                    setterMethods.add(optionalSetterBuilder.build());
                }

                return setterMethods.build();
            })
            .collect(Collectors.toList());
    }

    private static MethodSpec defineBuildMethod(
        TypeName configInterfaceType,
        String generatedClassName,
        List<ParameterSpec> constructorParameters,
        String configMapParameterName,
        Optional<MethodSpec> maybeFactoryFunction
    ) {
        String constructorParameterString = constructorParameters
            .stream()
            .map(param -> param.name)
            .collect(Collectors.joining(", "));

        var configCreateStatement = maybeFactoryFunction
            .map(factoryFunc -> CodeBlock.of("return $L.$L($L)",
                generatedClassName, factoryFunc.name, constructorParameterString))
            .orElse(CodeBlock.of("return new $L($L)", generatedClassName, constructorParameterString));

        return MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC)
            .addCode(CodeBlock.builder()
                .addStatement(
                    "$1T $2N = $1T.create(this.$3N)",
                    CypherMapWrapper.class,
                    configMapParameterName,
                    configMapParameterName
                ).addStatement(configCreateStatement).build())
            .returns(configInterfaceType).build();
    }

    private static TypeName unpackedType(TypeMirror returnType) {
        if (isTypeOf(Optional.class, returnType)) {
            var typeArguments = ((DeclaredType) returnType).getTypeArguments();
            if (!typeArguments.isEmpty()) {
                return ClassName.get(asTypeElement(typeArguments.get(0)));
            }
        }

        return TypeName.get(returnType);
    }

}
