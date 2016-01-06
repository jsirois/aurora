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
package org.apache.aurora.thrift.build;

import java.io.File;
import java.io.IOException;

import javax.annotation.Generated;

import com.facebook.swift.parser.model.BaseType;
import com.facebook.swift.parser.model.ConstValue;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.google.common.base.CaseFormat;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;

import org.slf4j.Logger;

import static java.util.Objects.requireNonNull;

class BaseEmitter {
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

  protected static final String AURORA_THRIFT_PACKAGE_NAME = "org.apache.aurora.thrift";

  private final Logger logger;
  private final File outdir;

  protected BaseEmitter(Logger logger, File outdir) {
    this.logger = logger;
    this.outdir = requireNonNull(outdir);
  }

  protected final Logger getLogger() {
    return logger;
  }

  protected final File getOutdir() {
    return outdir;
  }

  protected interface CodeBuilder {
    void build(CodeBlock.Builder builder);
  }

  protected static CodeBlock indented(CodeBuilder codeBuilder) {
    CodeBlock.Builder codeBlockBuilder = CodeBlock.builder();
    indented(codeBlockBuilder, () -> codeBuilder.build(codeBlockBuilder));
    return codeBlockBuilder.build();
  }

  protected static void indented(CodeBlock.Builder codeBlockBuilder, Runnable codeBuilder) {
    codeBlockBuilder.indent();
    codeBuilder.run();
    codeBlockBuilder.unindent();
  }

  protected interface LiteralFactory {
    CodeBlock create(ThriftType type, ConstValue value);
  }

  protected static String getterName(ThriftField field) {
    String upperCamelCaseFieldName = toUpperCamelCaseName(field);
    ThriftType type = field.getType();
    if (isBoolean(type)) {
      return "is" + upperCamelCaseFieldName;
    } else {
      return "get" + upperCamelCaseFieldName;
    }
  }

  protected static String witherName(ThriftField field) {
    return "with" + toUpperCamelCaseName(field);
  }

  protected static String setterName(ThriftField field) {
    return "set" + toUpperCamelCaseName(field);
  }

  protected static String isSetName(ThriftField field) {
    return "isSet" + toUpperCamelCaseName(field);
  }

  protected static String toUpperCamelCaseName(ThriftField field) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());
  }

  protected static String toUpperSnakeCaseName(ThriftField field) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName());
  }

  protected static boolean isBoolean(ThriftType type) {
    return type instanceof BaseType && ((BaseType) type).getType() == BaseType.Type.BOOL;
  }

  protected final TypeSpec writeType(String packageName, TypeSpec.Builder typeBuilder)
      throws IOException {

    TypeSpec type =
        typeBuilder.addAnnotation(
            AnnotationSpec.builder(Generated.class)
                .addMember("value", "$S", getClass().getName())
                .build())
            .build();

    JavaFile javaFile =
        JavaFile.builder(packageName, type)
            .addFileComment(APACHE_LICENSE)
            .indent("  ")
            .skipJavaLangImports(true)
            .build();
    javaFile.writeTo(getOutdir());
    getLogger().info("Wrote {} to {}", ClassName.get(packageName, type.name), getOutdir());
    return type;
  }
}
