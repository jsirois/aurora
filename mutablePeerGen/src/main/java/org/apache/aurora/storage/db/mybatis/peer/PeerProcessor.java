/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.storage.db.mybatis.peer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleTypeVisitor8;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import com.facebook.swift.codec.ThriftField;
import com.google.auto.value.AutoValue;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.apache.aurora.thrift.Annotation;
import org.apache.aurora.thrift.ThriftEntity;

import autovalue.shaded.com.google.common.common.collect.ImmutableMap;
import autovalue.shaded.com.google.common.common.collect.Maps;

public class PeerProcessor extends AbstractProcessor {

  private static AnnotationValue getAnnotationValue(AnnotationMirror annotationMirror, String name) {
    for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry
        : annotationMirror.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().toString().equals(name)) {
        return entry.getValue();
      }
    }
    throw new IllegalStateException();
  }

  private static String setterName(String fieldName) {
    return "set" + toUpperCamelCaseName(fieldName);
  }

  private static String toUpperCamelCaseName(String fieldName) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, fieldName);
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_8;
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return ImmutableSet.of(Annotation.class.getName());
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    ImmutableMap<TypeMirror, ? extends TypeElement> symbolTable =
        Maps.uniqueIndex(
            roundEnv.getElementsAnnotatedWith(Annotation.class).stream()
                .filter(e -> e instanceof TypeElement)
                .filter(e -> isAssignableFrom(ThriftEntity.class, e.asType()))
                .map(e -> ((TypeElement) e))
                .iterator(),
            Element::asType);

    for (TypeElement typeElement: symbolTable.values()) {
      Optional<PeerInfo> typePeerInfo = getPeerInfo(typeElement);
      if (typePeerInfo.isPresent() && typePeerInfo.get().render()) {
        TypeSpec.Builder typeSpec =
            TypeSpec.classBuilder(typePeerInfo.get().className().simpleName())
                .addAnnotation(
                    AnnotationSpec.builder(Generated.class)
                        .addMember("value", "$S", getClass().getName())
                        .build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());

        CodeBlock.Builder toThriftCode =
            CodeBlock.builder()
                .add("$[return $T.builder()", typeElement);

        for (Element member : typeElement.getEnclosedElements()) {
          Optional<AnnotationMirror> thriftFieldAnnotation =
              getAnnotation(ThriftField.class, member);
          if (thriftFieldAnnotation.isPresent()) {
            ExecutableElement method = (ExecutableElement) member;
            TypeMirror returnType = method.getReturnType();
            TypeElement symbol = symbolTable.get(returnType);

            Optional<PeerInfo> peerInfo = getPeerInfo(symbol);

            TypeName fieldType =
                peerInfo.isPresent() ? peerInfo.get().className() : TypeName.get(returnType);
            String fieldName =
                getAnnotationValue(thriftFieldAnnotation.get(), "name").getValue().toString();
            FieldSpec fieldSpec =
                FieldSpec.builder(fieldType, fieldName).addModifiers(Modifier.PRIVATE).build();

            CodeBlock code = CodeBlock.builder().add("$N", fieldSpec).build();
            toThriftCode.add("\n.$L(", setterName(fieldName));

            boolean isList = isAssignableFrom(List.class, returnType);
            boolean isMap = isAssignableFrom(Map.class, returnType);
            boolean isSet = isAssignableFrom(Set.class, returnType);
            if (isList || isMap || isSet) {
              ClassName collectionType;
              if (isList) {
                collectionType = ClassName.get(List.class);
              } else if (isMap) {
                collectionType = ClassName.get(Map.class);
              } else if (isSet) {
                collectionType = ClassName.get(Set.class);
              } else {
                throw new IllegalStateException();
              }

              ParameterizedTypeName parameterizedReturnTypeName = (ParameterizedTypeName) fieldType;
              DeclaredType declaredReturnType =
                  returnType.accept(new SimpleTypeVisitor8<DeclaredType, Void>() {
                    @Override
                    public DeclaredType visitDeclared(DeclaredType declaredType, Void v) {
                      return declaredType;
                    }
                  }, null);

              ParameterizedTypeName mutableCollectionType =
                  ParameterizedTypeName.get(
                      collectionType,
                      parameterizedReturnTypeName.typeArguments.toArray(new TypeName[0]));
              fieldSpec =
                  FieldSpec.builder(mutableCollectionType, fieldName)
                      .addModifiers(Modifier.PRIVATE)
                      .build();

              if (isList || isSet) {
                TypeMirror elementType = declaredReturnType.getTypeArguments().get(0);
                Optional<PeerInfo> elementPeerInfo = getPeerInfo(symbolTable.get(elementType));
                if (elementPeerInfo.isPresent()) {
                  mutableCollectionType =
                      ParameterizedTypeName.get(collectionType, elementPeerInfo.get().className());
                  fieldSpec =
                      FieldSpec.builder(mutableCollectionType, fieldName)
                          .addModifiers(Modifier.PRIVATE)
                          .build();

                  code =
                      CodeBlock.builder()
                          .add(
                              String.format(
                                  "$N.stream().map($T::toThrift).collect($T.%s())",
                                  isList ? "toList" : "toSet"),
                              fieldSpec,
                              elementPeerInfo.get().className(),
                              Collectors.class)
                          .build();
                }
              } // TODO(John Sirois): Handle maps with keys or values or both as structs.
            } else if (peerInfo.isPresent()) {
              code =
                  CodeBlock.builder()
                      .add("$N == null ? null : $N.toThrift()", fieldSpec, fieldSpec)
                      .build();
            }

            typeSpec.addField(fieldSpec);
            toThriftCode.add(code);
            toThriftCode.add(")");
          }
        }

        typeSpec.addMethod(
            MethodSpec.methodBuilder("toThrift")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .returns(TypeName.get(typeElement.asType()))
                .addCode(
                    toThriftCode
                        .add("\n.build();\n$]")
                        .build())
                .build());

        String packageName = typePeerInfo.get().className().packageName();
        JavaFile javaFile =
            JavaFile.builder(packageName, typeSpec.build())
                .indent("  ")
                .skipJavaLangImports(true)
                .build();
        try {
          javaFile.writeTo(processingEnv.getFiler());
        } catch (IOException e) {
          processingEnv.getMessager().printMessage(
              Diagnostic.Kind.ERROR,
              String.format("Error emitting peer for %s: %s", typeElement.getQualifiedName(), e),
              typeElement);
        }
      }
    }
    return true;
  }

  @AutoValue
  static abstract class PeerInfo {
    static PeerInfo create(String structPackageName, ClassName struct, String mutablePeerValue) {
      boolean render = Boolean.parseBoolean(mutablePeerValue);
      ClassName className;
      if (render) {
        String packageName = structPackageName + ".peer";
        className = ClassName.get(packageName, "Mutable" + struct.simpleName());
      } else {
        int i = mutablePeerValue.lastIndexOf('.');
        if (i == -1) {
          String packageName = "";
          className = ClassName.get(packageName, mutablePeerValue);
        } else {
          String packageName = mutablePeerValue.substring(0, i);
          className = ClassName.get(packageName, mutablePeerValue.substring(i + 1));
        }
      }
      return new AutoValue_PeerProcessor_PeerInfo(render, className);
    }

    abstract boolean render();
    abstract ClassName className();
  }

  private Optional<PeerInfo> getPeerInfo(@Nullable TypeElement typeElement) {
    if (typeElement == null) {
      return Optional.empty();
    }
    Optional<AnnotationMirror> annotation = getAnnotation(Annotation.class, typeElement);
    if (!annotation.isPresent()) {
      return Optional.empty();
    }
    AnnotationValue parameters = getAnnotationValue(annotation.get(), "value");
    for (Object val : (List<?>) parameters.getValue()) {
      AnnotationMirror parameter = (AnnotationMirror) val;
      String name = getAnnotationValue(parameter, "name").getValue().toString();
      if (name.equals("mutablePeer")) {
        String mutablePeerValue = getAnnotationValue(parameter, "value").getValue().toString();
        String packageName =
            getElementUtils().getPackageOf(typeElement).getQualifiedName().toString();
        PeerInfo peerInfo =
            PeerInfo.create(packageName, ClassName.get(typeElement), mutablePeerValue);
        return Optional.of(peerInfo);
      }
    }
    return Optional.empty();
  }

  private boolean isAssignableFrom(Class<?> baseClass, TypeMirror subClass) {
    Optional<TypeElement> typeElement =
        Optional.ofNullable(getElementUtils().getTypeElement(baseClass.getName()));
    if (!typeElement.isPresent()) {
      return false;
    }
    DeclaredType rawType = getTypeUtils().getDeclaredType(typeElement.get());
    return getTypeUtils().isAssignable(subClass, rawType);
  }

  private Optional<AnnotationMirror> getAnnotation(
      Class<? extends java.lang.annotation.Annotation> annotationType,
      Element element) {

    List<? extends AnnotationMirror> annotationMirrors = element.getAnnotationMirrors();
    for (AnnotationMirror annotationMirror : annotationMirrors) {
      if (isAssignableFrom(annotationType, annotationMirror.getAnnotationType())) {
        return Optional.of(annotationMirror);
      }
    }
    return Optional.empty();
  }

  private Elements getElementUtils() {
    return processingEnv.getElementUtils();
  }

  private Types getTypeUtils() {
    return processingEnv.getTypeUtils();
  }
}
