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
package org.apache.aurora.build.thrift;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.Document;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharSource;
import com.google.common.io.Files;

import org.slf4j.Logger;

public final class ThriftGen {
  private final File outdir;
  private final Optional<String> packageSuffix;
  private final Logger logger;

  public ThriftGen(File outdir, Optional<String> packageSuffix, Logger logger) {
    this.outdir = outdir;
    this.packageSuffix = packageSuffix;
    this.logger = logger;
  }

  public void generate(ImmutableSet<File> thriftFiles) throws IOException {
    SymbolTable symbolTable = new SymbolTable();
    processThriftFiles(symbolTable, thriftFiles, /* required */ false);
  }

  private SymbolTable processThriftFiles(
      SymbolTable symbolTable,
      ImmutableSet<File> thriftFiles,
      boolean required)
      throws IOException {

    Set<File> processed = new HashSet<>();
    for (File thriftFile : thriftFiles) {
      CharSource thriftIdl = Files.asCharSource(thriftFile, Charsets.UTF_8);
      Document document = ThriftIdlParser.parseThriftIdl(thriftIdl);
      String packageName = document.getHeader().getNamespace("java");
      if (packageName == null) {
        if (required) {
          throw new IllegalArgumentException(
              String.format("%s must declare a 'java' namespace", thriftFile.getPath()));
        } else {
          logger.warn("Skipping {} - no java namespace", thriftFile);
        }
      } else {
        symbolTable = symbolTable.updated(thriftFile, packageName, document.getDefinitions());
        ImmutableSet<File> includes =
            FluentIterable.from(document.getHeader().getIncludes())
                .transform(inc -> new File(thriftFile.getParentFile(), inc).getAbsoluteFile())
                .filter(f -> !processed.contains(f))
                .toSet();
        symbolTable = processThriftFiles(symbolTable, includes, /* required */ true);

        if (packageSuffix.isPresent()) {
          packageName = packageName + packageSuffix.get();
        }

        ThriftGenVisitor visitor =
            new ThriftGenVisitor(
                logger,
                outdir,
                symbolTable,
                packageName);
        document.visit(visitor);
        visitor.finish();
        processed.add(thriftFile);
      }
    }
    return symbolTable;
  }
}
