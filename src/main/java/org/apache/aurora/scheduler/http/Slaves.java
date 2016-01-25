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
package org.apache.aurora.scheduler.http;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.Storage;

import static java.util.Objects.requireNonNull;

/**
 * HTTP interface to serve as a HUD for the mesos slaves tracked in the scheduler.
 */
@Path("/slaves")
public class Slaves extends JerseyTemplateServlet {
  private final String clusterName;
  private final Storage storage;

  /**
   * Injected constructor.
   *
   * @param serverInfo server meta-data that contains the cluster name
   * @param storage store to fetch the host attributes from
   */
  @Inject
  public Slaves(ServerInfo serverInfo, Storage storage) {
    super("slaves");
    this.clusterName = checkNotBlank(serverInfo.getClusterName());
    this.storage = requireNonNull(storage);
  }

  private Iterable<HostAttributes> getHostAttributes() {
    return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes());
  }

  private static final Function<HostAttributes, Slave> TO_SLAVE =
      Slave::new;

  /**
   * Fetches the listing of known slaves.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get() {
    return fillTemplate(template -> {
      template.setAttribute("cluster_name", clusterName);

      template.setAttribute("slaves",
          FluentIterable.from(getHostAttributes()).transform(TO_SLAVE).toList());
    });
  }

  private static final Ordering<Attribute> ATTR_ORDER = Ordering.natural().onResultOf(
      new Function<Attribute, String>() {
        @Override
        public String apply(Attribute attr) {
          return attr .getName();
        }
      });

  /**
   * Template object to represent a slave.
   */
  private static class Slave {
    private final HostAttributes attributes;

    Slave(HostAttributes attributes) {
      this.attributes = attributes;
    }

    public String getHost() {
      return attributes.getHost();
    }

    public String getId() {
      return attributes.getSlaveId();
    }

    public MaintenanceMode getMode() {
      return attributes.getMode();
    }

    private static final Function<Attribute, String> ATTR_TO_STRING =
        attr -> attr.getName() + "=[" + Joiner.on(",").join(attr.getValues()) + "]";

    public String getAttributes() {
      return Joiner.on(", ").join(
          Iterables.transform(ATTR_ORDER.sortedCopy(attributes.getAttributes()), ATTR_TO_STRING));
    }
  }
}
