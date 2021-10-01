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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.Generated;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Map;

class HugeSparseArrayGenerator {

    private final Elements elementUtils;
    private final SourceVersion sourceVersion;

    HugeSparseArrayGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        this.elementUtils = elementUtils;
        this.sourceVersion = sourceVersion;
    }

    JavaFile generate(HugeSparseArrayValidation.Spec spec) {
        var typeSpec = generateTypeSpec(spec);
        return javaFile(spec, typeSpec);
    }

    private TypeSpec generateTypeSpec(HugeSparseArrayValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className());
        var valueType = TypeName.get(spec.valueType());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.FINAL)
            .addSuperinterface(TypeName.get(spec.element().asType()))
            .addOriginatingElement(spec.element());

        // class annotation
        builder.addAnnotation(generatedAnnotation());

        // class fields
        var pageShift = pageShiftField(spec.pageShift());
        var pageSize = pageSizeField(pageShift);
        var pageMask = pageMaskField(pageSize);
        builder.addField(pageShift);
        builder.addField(pageSize);
        builder.addField(pageMask);

        // instance fields
        var capacity = capacityField();
        var pages = pagesField(valueType);
        var defaultValue = defaultValueField(valueType);
        builder.addField(capacity);
        builder.addField(pages);
        builder.addField(defaultValue);

        // static methods
        var pageIndex = pageIndexMethod(pageShift);
        var indexInPage = indexInPageMethod(pageShift);
        builder.addMethod(pageIndex);
        builder.addMethod(indexInPage);

        // constructor
        builder.addMethod(constructor(valueType));

        // instance methods
        builder.addMethod(capacityMethod(capacity));
        builder.addMethod(getMethod(valueType, pages, pageIndex, indexInPage, defaultValue));
        builder.addMethod(containsMethod(valueType, pages, pageIndex, indexInPage, defaultValue));

        return builder.build();
    }

    private TypeName pagesTypes(TypeName valueType) {
        return ArrayTypeName.of(ArrayTypeName.of(valueType));
    }

    private AnnotationSpec generatedAnnotation() {
        return AnnotationSpec.builder(Generated.class)
            .addMember("value", "$S", HugeSparseArrayGenerator.class.getCanonicalName())
            .build();
    }

    private FieldSpec pageShiftField(int pageShift) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SHIFT", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$L", pageShift)
            .build();
    }

    private FieldSpec pageSizeField(FieldSpec pageShiftField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SIZE", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("1 << $N", pageShiftField)
            .build();
    }

    private FieldSpec pageMaskField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_MASK", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$N - 1", pageSizeField)
            .build();
    }


    private FieldSpec capacityField() {
        return FieldSpec
            .builder(TypeName.LONG, "capacity", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private FieldSpec pagesField(TypeName valueType) {
        return FieldSpec
            .builder(pagesTypes(valueType), "pages", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private FieldSpec defaultValueField(TypeName valueType) {
        return FieldSpec
            .builder(valueType, "defaultValue", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private MethodSpec constructor(TypeName valueType) {
        return MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(TypeName.LONG, "capacity")
            .addParameter(pagesTypes(valueType), "pages")
            .addParameter(valueType, "defaultValue")
            .addStatement("this.capacity = capacity")
            .addStatement("this.pages = pages")
            .addStatement("this.defaultValue = defaultValue")
            .build();
    }

    private MethodSpec pageIndexMethod(FieldSpec pageShift) {
        return MethodSpec.methodBuilder("pageIndex")
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.INT)
            .addStatement("return (int) (index >>> $N)", pageShift)
            .build();
    }

    private MethodSpec indexInPageMethod(FieldSpec pageMask) {
        return MethodSpec.methodBuilder("indexInPage")
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.INT)
            .addStatement("return (int) (index & $N)", pageMask)
            .build();
    }

    private MethodSpec capacityMethod(FieldSpec capacityField) {
        return MethodSpec.methodBuilder("capacity")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .addStatement("return $N", capacityField)
            .build();
    }

    private MethodSpec getMethod(
        TypeName valueType,
        FieldSpec pages,
        MethodSpec pageIndex,
        MethodSpec indexInPage,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("get")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(valueType)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
                .beginControlFlow("if (pageIndex < $N.length)", pages)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .addStatement("return page[indexInPage]")
                .endControlFlow()
                .endControlFlow()
                .addStatement("return $N", defaultValue)
                .build())
            .build();
    }

    private static final Map<TypeName, String> EQUALITY_FUNCTIONS = Map.of(
        TypeName.BYTE, "page[indexInPage] != $N",
        TypeName.SHORT, "page[indexInPage] != $N",
        TypeName.INT, "page[indexInPage] != $N",
        TypeName.LONG, "page[indexInPage] != $N",
        TypeName.FLOAT, "Float.compare(page[indexInPage], $N) != 0",
        TypeName.DOUBLE, "Double.compare(page[indexInPage], $N) != 0"
    );

    private MethodSpec containsMethod(
        TypeName valueType,
        FieldSpec pages,
        MethodSpec pageIndex,
        MethodSpec indexInPage,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("contains")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.BOOLEAN)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .addStatement("return " + EQUALITY_FUNCTIONS.get(valueType), defaultValue)
                .endControlFlow()
                .addStatement("return false")
                .build())
            .build();
    }

    private JavaFile javaFile(HugeSparseArrayValidation.Spec spec, TypeSpec typeSpec) {
        return JavaFile
            .builder(spec.rootPackage().toString(), typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }
}
