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
package org.apache.aurora.build;

import java.util.Map;

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.ConstValue;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftException;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.Union;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.squareup.javapoet.CodeBlock;

abstract class AbstractStructRenderer {
  private static class StructRenderer extends AbstractStructRenderer {
    private StructRenderer(AbstractStruct struct) {
      super(struct);
    }

    @Override
    CodeBlock createLiteral(
        ImmutableMap<String, ConstValue> parameters,
        BaseEmitter.LiteralFactory literalFactory) {

      return BaseEmitter.indented(codeBuilder -> {
        codeBuilder.add("$L.builder()", name);
        for (ThriftField field : fields) {
          String fieldName = field.getName();
          if (parameters.containsKey(fieldName)) {
            ConstValue fieldValue = parameters.get(fieldName);
            codeBuilder.add("\n.$L(", BaseEmitter.setterName(field));
            codeBuilder.add(literalFactory.create(field.getType(), fieldValue));
            codeBuilder.add(")");
          }
        }
        codeBuilder.add("\n.build()");
      });
    }
  }

  private static class UnionRenderer extends AbstractStructRenderer {
    private UnionRenderer(AbstractStruct struct) {
      super(struct);
    }

    @Override
    CodeBlock createLiteral(
        ImmutableMap<String, ConstValue> parameters,
        BaseEmitter.LiteralFactory literalFactory) {

      Map.Entry<String, ConstValue> element =
          Iterables.getOnlyElement(parameters.entrySet());
      String elementName = element.getKey();
      ThriftField elementField =
          Maps.uniqueIndex(fields, ThriftField::getName).get(elementName);
      if (elementField == null) {
        throw new ParseException(
            String.format(
                "Encountered a union literal that selects a non-existent member '%s'.\n" +
                    "Only the following members are known:\n\t%s",
                elementName,
                Joiner.on("\n\t").join(fields.stream().map(ThriftField::getName).iterator())));
      }
      ThriftType elementType = elementField.getType();

      return CodeBlock.builder()
          .add("new $L(", name)
          .add(literalFactory.create(elementType, element.getValue()))
          .add(")")
          .build();
    }
  }

  static AbstractStructRenderer from(AbstractStruct struct) {
    if (struct instanceof Struct || struct instanceof ThriftException) {
      return new StructRenderer(struct);
    } else if (struct instanceof Union) {
      return new UnionRenderer(struct);
    } else {
      throw new UnexpectedTypeException("Unknown struct type: " + struct);
    }
  }

  protected final String name;
  protected final ImmutableList<ThriftField> fields;

  private AbstractStructRenderer(AbstractStruct struct) {
    this.name = struct.getName();
    this.fields = ImmutableList.copyOf(struct.getFields());
  }

  abstract CodeBlock createLiteral(
      ImmutableMap<String, ConstValue> parameters,
      BaseEmitter.LiteralFactory literalFactory);
}
