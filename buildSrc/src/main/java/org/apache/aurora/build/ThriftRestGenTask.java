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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.Document;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.CharSource;
import com.google.common.io.Files;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

/**
 * Generates thrift stubs for structs and services.
 */
public class ThriftRestGenTask extends DefaultTask {

  private Optional<String> packageSuffix = Optional.absent();

  @Input
  public void setPackageSuffix(String packageSuffix) {
    String normalizedSuffix = packageSuffix.trim();
    if (normalizedSuffix.isEmpty()) {
      throw new IllegalArgumentException("Invalid packageSuffix, cannot be blank.");
    }
    this.packageSuffix = Optional.of(normalizedSuffix);
  }

  @TaskAction
  public void gen() throws IOException {
    // TODO(John Sirois): The parser does not carry over doc comments and we want these for the
    // rest api, investigate a patch to add support before copying/moving comments to annotations.

    // TODO(John Sirois): The parser does not carry over annotations in all possible locations -
    // we may want this - partially depends on TODO above.

    File outdir = getOutputs().getFiles().getSingleFile();
    SymbolTable symbolTable = new SymbolTable();
    Set<File> thriftFiles =
        getInputs().getFiles().getFiles()
            .stream()
            .map(File::getAbsoluteFile)
            .collect(Collectors.toSet());
    processThriftFiles(symbolTable, thriftFiles, outdir, false);
  }

  private SymbolTable processThriftFiles(
      SymbolTable symbolTable,
      Set<File> thriftFiles,
      File outdir,
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
          getLogger().warn("Skipping {} - no java namespace", thriftFile);
        }
      } else {
        symbolTable = symbolTable.updated(thriftFile, packageName, document.getDefinitions());
        Set<File> includes =
            document.getHeader().getIncludes()
                .stream()
                .map(inc -> new File(thriftFile.getParentFile(), inc).getAbsoluteFile())
                .filter(f -> !processed.contains(f))
                .collect(Collectors.toSet());
        symbolTable = processThriftFiles(symbolTable, includes, outdir, true);

        if (packageSuffix.isPresent()) {
          packageName = packageName + packageSuffix.get();
        }

        ThriftGenVisitor visitor =
            new ThriftGenVisitor(
                getLogger(),
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
