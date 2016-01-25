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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.apache.aurora.thrift.ThriftAnnotation;
import org.apache.aurora.thrift.ThriftAnnotations;
import org.apache.aurora.thrift.ThriftEntity;

/**
 * Generates mutable peers from immutable thrift structs for use by MyBatis during hydration.
 * <p>
 * MyBatis expects mapped entities to be singular; ie: each mapped entity class must have both
 * accessors and mutators housed on the entity class.  This precludes the use of immutable objects
 * with builders as database mapping targets and it precludes mapping of immutable objects in
 * general since MyBatis constructor mapping support is limited (it cannot handle collections).
 * Aurora has worked around this and other mapping difficulties with hand-made peer classes
 * ("views").  These classes use the pattern of "exposing" mutators via private fields (MyBatis is
 * built to be able to reflect private members) and providing a single "toThrift" public method
 * that produces the peer thrift object using the private fields populated by MyBatis.
 * <p>
 * This annotation processor automates peer hand coding and generates mutable peers for any thrift
 * struct annotated with a "mutablePeer" value.  If the value is "true" a mutable peer is generated.
 * Otherwise, the value is treated as a fully qualified classname pointing to a hand-coded mutable
 * peer and this type is expected to have a "toThrift" method that works like the generated mutable
 * peers.  The end result is that mutable object graphs created by MyBatis can be converted to
 * immutable thrift graphs by calling "toThrift" on the root peer.
 */
public class MutablePeerProcessor extends AbstractProcessor {

  // TODO(John Sirois): Load this from a resource.
  private static final String APACHE_LICENSE =
      " Licensed under the Apache License, Version 2.0 (the \"License\");\n" +
      " you may not use this file except in compliance with the License.\n" +
      " You may obtain a copy of the License at\n" +
      "\n" +
      "     http://www.apache.org/licenses/LICENSE-2.0\n" +
      "\n" +
      " Unless required by applicable law or agreed to in writing, software\n" +
      " distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
      " WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
      " See the License for the specific language governing permissions and\n" +
      " limitations under the License.";

  @VisibleForTesting
  static final String MUTABLE_PEER_MAPS_NOT_SUPPORTED_MSG =
      "Mutable peers are not yet supported for structs containing Map fields where either the " +
      "key or value is itself a mutable peer.";

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
    return ImmutableSet.of(ThriftAnnotations.class.getName(), ThriftAnnotation.class.getName());
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    ImmutableMap<TypeMirror, TypeElement> symbolTable =
        Maps.uniqueIndex(
            Sets.union(
                roundEnv.getElementsAnnotatedWith(ThriftAnnotations.class),
                roundEnv.getElementsAnnotatedWith(ThriftAnnotation.class))
                .stream()
                .filter(e -> isAssignableFrom(ThriftEntity.class, e.asType()))
                .filter(TypeElement.class::isInstance)
                .map(TypeElement.class::cast)
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
          Optional<? extends AnnotationMirror> thriftFieldAnnotation =
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
              } else {
                collectionType = ClassName.get(Set.class);
              }

              ParameterizedTypeName parametrizedReturnTypeName = (ParameterizedTypeName) fieldType;
              DeclaredType declaredReturnType =
                  returnType.accept(new SimpleTypeVisitor8<DeclaredType, Void>() {
                    @Override public DeclaredType visitDeclared(DeclaredType declaredType, Void v) {
                      return declaredType;
                    }
                  }, null);

              ParameterizedTypeName mutableCollectionType =
                  ParameterizedTypeName.get(
                      collectionType,
                      parametrizedReturnTypeName.typeArguments.toArray(
                          new TypeName[parametrizedReturnTypeName.typeArguments.size()]));
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
              } else {
                TypeMirror keyType = declaredReturnType.getTypeArguments().get(0);
                Optional<PeerInfo> keyPeerInfo = getPeerInfo(symbolTable.get(keyType));

                TypeMirror valueType = declaredReturnType.getTypeArguments().get(1);
                Optional<PeerInfo> valuePeerInfo = getPeerInfo(symbolTable.get(valueType));

                if (keyPeerInfo.isPresent() || valuePeerInfo.isPresent()) {
                  // TODO(John Sirois): Add similar support to list and set above only as-needed.
                  processingEnv.getMessager().printMessage(
                      Diagnostic.Kind.ERROR,
                      MUTABLE_PEER_MAPS_NOT_SUPPORTED_MSG,
                      member);
                }
              }
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
                .addFileComment(APACHE_LICENSE)
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

  // TODO(John Sirois): Use AutoValue here.  Right now, its problematic for `./gradlew idea` to have
  // annotation processors use other annotation processors, preventing this use.
  static class PeerInfo {
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
      return new PeerInfo(render, className);
    }

    private final boolean render;
    private final ClassName className;

    private PeerInfo(boolean render, ClassName className) {
      this.render = render;
      this.className = className;
    }

    boolean render() {
      return render;
    }

    ClassName className() {
      return className;
    }
  }

  private Optional<PeerInfo> getPeerInfo(@Nullable TypeElement typeElement) {
    if (typeElement == null) {
      return Optional.empty();
    }
    for (AnnotationMirror thriftAnnotation : getThriftAnnotations(typeElement)) {
      String name = getAnnotationValue(thriftAnnotation, "name").getValue().toString();
      if (name.equals("mutablePeer")) {
        String mutablePeerValue =
            getAnnotationValue(thriftAnnotation, "value").getValue().toString();
        String packageName =
            getElementUtils().getPackageOf(typeElement).getQualifiedName().toString();
        PeerInfo peerInfo =
            PeerInfo.create(packageName, ClassName.get(typeElement), mutablePeerValue);
        return Optional.of(peerInfo);
      }
    }
    return Optional.empty();
  }

  private Iterable<? extends AnnotationMirror> getThriftAnnotations(TypeElement typeElement) {
    Optional<? extends AnnotationMirror> container =
        getAnnotation(ThriftAnnotations.class, typeElement);
    if (container.isPresent()) {
      @SuppressWarnings("unchecked") // We know @ThriftAnnotations.value is a ThriftAnnotation[].
      List<? extends AnnotationValue> values =
          (List<? extends AnnotationValue>) getAnnotationValue(container.get(), "value").getValue();
      return values.stream()
          .map(AnnotationValue::getValue)
          // We know @ThriftAnnotations.value is a ThriftAnnotation[].
          .map(AnnotationMirror.class::cast)
          .collect(Collectors.toList());
    } else {
      return getAnnotations(ThriftAnnotation.class, typeElement);
    }
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

  private Optional<? extends AnnotationMirror> getAnnotation(
      Class<? extends java.lang.annotation.Annotation> annotationType,
      Element element) {

    return Optional.ofNullable(
        Iterables.getOnlyElement(getAnnotations(annotationType, element), null));
  }

  private Iterable<? extends AnnotationMirror> getAnnotations(
      Class<? extends java.lang.annotation.Annotation> annotationType,
      Element element) {

    return FluentIterable.from(element.getAnnotationMirrors())
        .filter(am -> isAssignableFrom(annotationType, am.getAnnotationType()));
  }

  private Elements getElementUtils() {
    return processingEnv.getElementUtils();
  }

  private Types getTypeUtils() {
    return processingEnv.getTypeUtils();
  }
}