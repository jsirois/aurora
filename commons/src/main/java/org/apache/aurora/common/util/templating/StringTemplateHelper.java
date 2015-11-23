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
package org.apache.aurora.common.util.templating;

import java.io.IOException;
import java.io.Writer;
import java.net.URL;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Resources;

import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.base.MorePreconditions;
import org.stringtemplate.v4.AutoIndentWriter;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.misc.STMessage;

/**
 * A class to simplify the operations required to load a stringtemplate template file from the
 * classpath and populate it.
 */
public class StringTemplateHelper {

  /**
   * Thrown when an exception is encountered while populating a template.
   */
  public static class TemplateException extends RuntimeException {
    public TemplateException(String msg, Throwable cause) {
      super(msg, cause);
    }

    public TemplateException(IOException e) {
      super(e);
    }

    public TemplateException(String msg) {
      super(msg);
    }
  }

  private static class ErrorListener implements STErrorListener {
    private final URL templateUrl;

    public ErrorListener(URL templateUrl) {
      this.templateUrl = templateUrl;
    }

    @Override
    public void compileTimeError(STMessage msg) {
      raise("compile", msg);
    }

    @Override
    public void runTimeError(STMessage msg) {
      raise("runtime", msg);
    }

    @Override
    public void IOError(STMessage msg) {
      raise("io", msg);
    }

    @Override
    public void internalError(STMessage msg) {
      raise("internal", msg);
    }

    void raise(String category, STMessage errorMessage) {
      throw new TemplateException(
          String.format("[%s] Failure in %s: %s", category, templateUrl, errorMessage.toString()),
          errorMessage.cause);
    }
  }

  private final Supplier<ST> prototypeTemplate;

  /**
   * Creates a new template helper.
   *
   * @param templateContextClass Classpath context for the location of the template file.
   * @param templateName Template file name (excluding .st suffix) relative to
   *     {@code templateContextClass}.
   * @param cacheTemplates Whether the template should be cached.
   */
  public StringTemplateHelper(
      Class<?> templateContextClass,
      String templateName,
      boolean cacheTemplates) {

    MorePreconditions.checkNotBlank(templateName);
    URL templateResource = Resources.getResource(templateContextClass, templateName + ".st");

    STGroup group = new STGroup('$', '$');
    group.setListener(new ErrorListener(templateResource));
    Supplier<ST> stSupplier = () -> {
      try {
        String template = Resources.toString(templateResource, Charsets.UTF_8);
        return new ST(group, template);
      } catch (IOException e) {
        throw new TemplateException(
            String.format("Failed to read template at %s", templateResource),
            e);
      }
    };
    prototypeTemplate = cacheTemplates ? Suppliers.memoize(stSupplier): stSupplier;
  }

  /**
   * Writes the populated template to an output writer by providing a closure with access to
   * the unpopulated template object.
   *
   * @param out Template output writer.
   * @param parameterSetter Closure to populate the template.
   * @throws TemplateException If an exception was encountered while populating the template.
   */
  public void writeTemplate(Writer out, Closure<ST> parameterSetter) throws TemplateException {
    Preconditions.checkNotNull(out);
    Preconditions.checkNotNull(parameterSetter);

    ST stringTemplate = new ST(prototypeTemplate.get());
    try {
      parameterSetter.execute(stringTemplate);
      stringTemplate.write(new AutoIndentWriter(out));
    } catch (IOException e) {
      throw new TemplateException(e);
    }
  }
}
