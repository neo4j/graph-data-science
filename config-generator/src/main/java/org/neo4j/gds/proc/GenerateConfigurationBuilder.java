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

import javax.annotation.processing.Messager;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreTypes.isTypeOf;
import static org.neo4j.gds.proc.TypeUtil.unwrapOptionalType;

class GenerateConfigurationBuilder {
    private final Messager messager;

    GenerateConfigurationBuilder(Messager messager) {this.messager = messager;}

    TypeSpec defineConfigBuilder(
        ConfigParser.Spec config,
        String packageName,
        String generatedClassName,
        List<ParameterSpec> constructorParameters,
        String configMapParameterName,
        Optional<MethodSpec> maybeFactoryFunction
    ) {
        ClassName builderClassName = ClassName.get(packageName, generatedClassName + ".Builder");
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

        // FIXME correct handling of ConvertWith function (use type of convertWith on the builder function instead !

        return configBuilderClass
            .addMethods(defineConfigParameterSetters(config.members(), builderClassName))
            .addMethods(defineConfigMapEntrySetters(config.members(), configMapParameterName, builderClassName))
            .addMethod(defineBuildMethod(config, generatedClassName, constructorParameters, configMapParameterName, maybeFactoryFunction))
            .build();
    }

    private static MethodSpec defineBuildMethod(
        ConfigParser.Spec config,
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
            .returns(TypeName.get(config.rootType())).build();
    }

    private static List<MethodSpec> defineConfigParameterSetters(
        List<ConfigParser.Member> members,
        ClassName builderClassName
    ) {
        // if member -> get actual type by checking whatever the convert with method has
        return members.stream()
            .filter(ConfigParser.Member::isConfigParameter)
            .map(member -> MethodSpec.methodBuilder(member.methodName())
                .addModifiers(Modifier.PUBLIC)
                .addParameter(TypeName.get(member.method().getReturnType()), member.methodName())
                .returns(builderClassName)
                .addCode(CodeBlock.builder()
                    .addStatement("this.$1N = $1N", member.methodName())
                    .addStatement("return this")
                    .build()
                ).build()
            )
            .collect(Collectors.toList());
    }

    private List<MethodSpec> defineConfigMapEntrySetters(
        List<ConfigParser.Member> members,
        String builderConfigMapFieldName,
        ClassName builderClassName
    ) {
        return members.stream()
            .filter(ConfigParser.Member::isConfigMapEntry)
            .flatMap(member -> {
                // TODO if get actual type by checking whatever the convertwith method has supports
                var setterMethods = Stream.<MethodSpec>builder();
                MethodSpec.Builder setMethodBuilder = MethodSpec.methodBuilder(member.methodName())
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(configEntryValueType(member), member.methodName())
                    .returns(builderClassName)
                    .addCode(CodeBlock.builder()
                        .addStatement(
                            "this.$N.put(\"$L\", $N)",
                            builderConfigMapFieldName,
                            member.lookupKey(),
                            member.methodName()
                        )
                        .addStatement("return this")
                        .build()
                    );

                setterMethods.add(setMethodBuilder.build());
                
                if (isTypeOf(Optional.class, member.method().getReturnType())) {
                    String lambdaVarName = "actual" + member.methodName();

                    var optionalSetterBuilder = MethodSpec.methodBuilder(member.methodName())
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(TypeName.get(member.method().getReturnType()), member.methodName())
                        .returns(builderClassName)
                        .addStatement(
                            "$1N.ifPresent($2N -> this.$3N.put(\"$4L\", $2N))",
                            member.methodName(),
                            lambdaVarName,
                            builderConfigMapFieldName,
                            member.lookupKey()
                        ).addStatement("return this");
                    
                    setterMethods.add(optionalSetterBuilder.build());
                }

                return setterMethods.build();
            })
            .collect(Collectors.toList());
    }

    private TypeName configEntryValueType(ConfigParser.Member member) {
        TypeMirror returnType = member.method().getReturnType();
        
        if (isTypeOf(Optional.class, returnType)) {
            Optional<ClassName> maybeType = unwrapOptionalType(member, (DeclaredType) returnType, messager);
            if (maybeType.isPresent()) {
                return maybeType.get();
            }
        }

        return TypeName.get(returnType);
    }
}
