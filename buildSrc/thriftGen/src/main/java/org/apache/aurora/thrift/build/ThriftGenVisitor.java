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

import javax.annotation.concurrent.NotThreadSafe;

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.Const;
import com.facebook.swift.parser.model.IntegerEnum;
import com.facebook.swift.parser.model.Service;
import com.facebook.swift.parser.model.StringEnum;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftException;
import com.facebook.swift.parser.model.Typedef;
import com.facebook.swift.parser.model.Union;
import com.facebook.swift.parser.visitor.DocumentVisitor;
import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.ClassName;

import org.slf4j.Logger;

@NotThreadSafe
class ThriftGenVisitor implements DocumentVisitor {
  private final ImmutableMap.Builder<ClassName, AbstractStructRenderer> structRendererByName =
      ImmutableMap.builder();

  private final ImmutableMap<Class<? extends Visitable>, Visitor<? extends Visitable>> visitors;

  private final String packageName;
  private boolean finished;

  ThriftGenVisitor(
      Logger logger,
      File outdir,
      SymbolTable symbolTable,
      String packageName) {

    this.packageName = packageName;
    visitors =
        ImmutableMap.<Class<? extends Visitable>, Visitor<? extends Visitable>>builder()
            .put(Const.class,
                new ConstVisitor(logger, outdir, symbolTable, packageName))
            .put(IntegerEnum.class,
                new IntegerEnumVisitor(logger, outdir, symbolTable, packageName))
            .put(Service.class,
                new ServiceVisior(logger, outdir, symbolTable, packageName))
            // Not needed by Aurora and of questionable value to ever add support for.
            .put(StringEnum.class,
                Visitor.failing("The Senum type is deprecated and removed in thrift 1.0.0, " +
                    "see: https://issues.apache.org/jira/browse/THRIFT-2003"))
            .put(Struct.class,
                new StructVisitor(
                    logger,
                    outdir,
                    symbolTable,
                    packageName))
            // Currently not used by Aurora, but trivial to support.
            .put(ThriftException.class, Visitor.failing())
            // TODO(John Sirois): Implement as the need arises.
            // Currently not needed by Aurora; requires deferring all generation to `finish` and
            // collecting a full symbol table + adding a resolve method to resolve through
            // typedefs.
            .put(Typedef.class, Visitor.failing())
            .put(Union.class,
                new UnionVisitor(
                    logger,
                    outdir,
                    symbolTable,
                    packageName))
            .build();
  }

  @Override
  public boolean accept(Visitable visitable) {
    return visitors.containsKey(visitable.getClass()) || visitable instanceof AbstractStruct;
  }

  // We only accept visitables we have a type-matching visitor for above; so the raw typed
  // `visitor.visit(visitable);` call below is safe.
  @SuppressWarnings("unchecked")
  @Override
  public void visit(Visitable visitable) throws IOException {
    if (visitable instanceof AbstractStruct) {
      AbstractStruct struct = (AbstractStruct) visitable;
      structRendererByName.put(
          ClassName.get(packageName, struct.getName()),
          AbstractStructRenderer.from(struct));
    }
    Visitor visitor = visitors.get(visitable.getClass());
    if (visitor != null) {
      visitor.visit(visitable);
    }
  }

  @Override
  public void finish() throws IOException {
    if (!finished) {
      ImmutableMap<ClassName, AbstractStructRenderer> structRenderers =
          structRendererByName.build();

      for (Visitor<?> visitor : visitors.values()) {
        visitor.finish(structRenderers);
      }
      finished = true;
    }
  }
}
