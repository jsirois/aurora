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
package org.apache.aurora.scheduler.filter;

import java.util.Arrays;
import java.util.Set;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType;
import org.apache.aurora.scheduler.mesos.Offers;
import org.apache.aurora.scheduler.mesos.TaskExecutors;
import org.junit.Before;
import org.junit.Test;

import autovalue.shaded.com.google.common.common.collect.Lists;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.CPU;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.DISK;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.PORTS;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.RAM;
import static org.junit.Assert.assertEquals;

public class SchedulingFilterImplTest extends EasyMockTest {
  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String HOST_C = "hostC";

  private static final String RACK_A = "rackA";
  private static final String RACK_B = "rackB";

  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";

  private static final JobKey JOB_A = JobKeys.from("roleA", "env", "jobA");
  private static final JobKey JOB_B = JobKeys.from("roleB", "env", "jobB");

  private static final int DEFAULT_CPUS = 4;
  private static final long DEFAULT_RAM = 1000;
  private static final long DEFAULT_DISK = 2000;
  private static final ResourceSlot DEFAULT_OFFER = Resources.from(
      Offers.createOffer(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK, Pair.of(80, 80))).slot();

  private SchedulingFilter defaultFilter;

  @Before
  public void setUp() {
    defaultFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
  }

  @Test
  public void testMeetsOffer() {
    control.replay();

    HostAttributes attributes = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertNoVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK), attributes);
    assertNoVetoes(
        makeTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1),
        attributes);
  }

  @Test
  public void testSufficientPorts() {
    control.replay();

    ResourceSlot twoPorts = Resources.from(
        Offers.createOffer(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK, Pair.of(80, 81))).slot();

    TaskConfig noPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .withRequestedPorts(ImmutableSet.of());
    TaskConfig onePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .withRequestedPorts(ImmutableSet.of("one"));
    TaskConfig twoPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .withRequestedPorts(ImmutableSet.of("one", "two"));
    TaskConfig threePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .withRequestedPorts(ImmutableSet.of("one", "two", "three"));

    Set<Veto> none = ImmutableSet.of();
    HostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(noPortTask, EMPTY)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(onePortTask, EMPTY)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(twoPortTask, EMPTY)));
    assertEquals(
        ImmutableSet.of(PORTS.veto(1)),
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(threePortTask, EMPTY)));
  }

  @Test
  public void testInsufficientResources() {
    control.replay();

    HostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertVetoes(
        makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1),
        hostA,
        CPU.veto(1), DISK.veto(1), RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK), hostA, CPU.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK), hostA, RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1), hostA, DISK.veto(1));
  }

  @Test
  public void testDedicatedRole() {
    control.replay();

    HostAttributes hostA = hostAttributes(HOST_A, dedicated(JOB_A.getRole()));
    checkConstraint(hostA, DEDICATED_ATTRIBUTE, true, JOB_A.getRole());
    assertVetoes(makeTask(JOB_B), hostA, Veto.dedicatedHostConstraintMismatch());
  }

  @Test
  public void testSharedDedicatedHost() {
    control.replay();

    String dedicated1 = dedicatedFor(JOB_A);
    String dedicated2 = dedicatedFor(JOB_B);
    HostAttributes hostA = hostAttributes(HOST_A, dedicated(dedicated1, dedicated2));
    assertNoVetoes(
        checkConstraint(
            JOB_A,
            hostA,
            DEDICATED_ATTRIBUTE,
            true,
            dedicated1),
        hostA);
    assertNoVetoes(
        checkConstraint(
            JOB_B,
            hostA,
            DEDICATED_ATTRIBUTE,
            true,
            dedicated2),
        hostA);
  }

  @Test
  public void testMultiValuedAttributes() {
    control.replay();

    HostAttributes hostA = hostAttributes(HOST_A, valueAttribute("jvm", "1.0", "2.0", "3.0"));
    checkConstraint(hostA, "jvm", true, "1.0");
    checkConstraint(hostA, "jvm", false, "4.0");

    checkConstraint(hostA, "jvm", true, "1.0", "2.0");
    HostAttributes hostB = hostAttributes(HOST_A, valueAttribute("jvm", "1.0"));
    checkConstraint(hostB, "jvm", false, "2.0", "3.0");
  }

  @Test
  public void testHostScheduledForMaintenance() {
    control.replay();

    assertNoVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.SCHEDULED, host(HOST_A), rack(RACK_A)));
  }

  @Test
  public void testHostDrainingForMaintenance() {
    control.replay();

    assertVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.DRAINING, host(HOST_A), rack(RACK_A)),
        Veto.maintenance("draining"));
  }

  @Test
  public void testHostDrainedForMaintenance() {
    control.replay();

    assertVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.DRAINED, host(HOST_A), rack(RACK_A)),
        Veto.maintenance("drained"));
  }

  @Test
  public void testMultipleTaskConstraints() {
    control.replay();

    Constraint constraint1 = makeConstraint("host", HOST_A);
    Constraint constraint2 = makeConstraint(DEDICATED_ATTRIBUTE, "xxx");

    assertVetoes(
        makeTask(JOB_A, constraint1, constraint2),
        hostAttributes(HOST_A, dedicated(HOST_A), host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertNoVetoes(
        makeTask(JOB_B, constraint1, constraint2),
        hostAttributes(HOST_B, dedicated("xxx"), host(HOST_A)));
  }

  @Test
  public void testDedicatedMismatchShortCircuits() {
    // Ensures that a dedicated mismatch short-circuits other filter operations, such as
    // evaluation of limit constraints.  Reduction of task queries is the desired outcome.

    control.replay();

    Constraint hostLimit = limitConstraint("host", 1);
    assertVetoes(
        makeTask(JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_A, host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertVetoes(
        makeTask(JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_B, dedicated(dedicatedFor(JOB_B)), host(HOST_B)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
  }

  @Test
  public void testUnderLimitNoTasks() {
    control.replay();

    assertNoVetoes(hostLimitTask(2), hostAttributes(HOST_A, host(HOST_A)));
  }

  private Attribute host(String host) {
    return valueAttribute(HOST_ATTRIBUTE, host);
  }

  private Attribute rack(String rack) {
    return valueAttribute(RACK_ATTRIBUTE, rack);
  }

  private Attribute dedicated(String value, String... values) {
    return valueAttribute(DEDICATED_ATTRIBUTE, value, values);
  }

  private String dedicatedFor(IJobKey job) {
    return job.getRole() + "/" + job.getName();
  }

  @Test
  public void testLimitWithinJob() throws Exception {
    control.replay();

    AttributeAggregate stateA = AttributeAggregate.create(
        Suppliers.ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_C),
            rack(RACK_B))));
    AttributeAggregate stateB = AttributeAggregate.create(
        Suppliers.ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A))));

    HostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    HostAttributes hostB = hostAttributes(HOST_B, host(HOST_B), rack(RACK_A));
    HostAttributes hostC = hostAttributes(HOST_C, host(HOST_C), rack(RACK_B));
    assertNoVetoes(hostLimitTask(JOB_A, 2), hostA, stateA);
    assertVetoes(
        hostLimitTask(JOB_A, 1),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertVetoes(
        hostLimitTask(JOB_A, 2),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertNoVetoes(hostLimitTask(JOB_A, 3), hostB, stateA);

    assertVetoes(
        rackLimitTask(JOB_A, 2),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertVetoes(
        rackLimitTask(JOB_A, 3),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(JOB_A, 4), hostB, stateB);

    assertNoVetoes(rackLimitTask(JOB_A, 1), hostC, stateB);

    assertVetoes(
        rackLimitTask(JOB_A, 1),
        hostC,
        stateA,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(JOB_A, 2), hostC, stateB);
  }

  @Test
  public void testAttribute() {
    control.replay();

    HostAttributes hostA = hostAttributes(HOST_A, valueAttribute("jvm", "1.0"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.0");

    // Matches attribute, different value.
    checkConstraint(hostA, "jvm", false, "1.4");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.4");

    // Logical 'OR' matching attribute.
    checkConstraint(hostA, "jvm", false, "1.2", "1.4");

    // Logical 'OR' not matching attribute.
    checkConstraint(hostA, "xxx", false, "1.0", "1.4");
  }

  @Test
  public void testAttributes() {
    control.replay();

    HostAttributes hostA = hostAttributes(
        HOST_A,
        valueAttribute("jvm", "1.4", "1.6", "1.7"),
        valueAttribute("zone", "a", "b", "c"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.4");

    // Matches attribute, different value.
    checkConstraint(hostA, "jvm", false, "1.0");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.4");

    // Logical 'OR' with attribute and value match.
    checkConstraint(hostA, "jvm", true, "1.2", "1.4");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.0", "1.4");

    // Check that logical AND works.
    Constraint jvmConstraint = makeConstraint("jvm", "1.6");
    Constraint zoneConstraint = makeConstraint("zone", "c");

    TaskConfig task = makeTask(JOB_A, jvmConstraint, zoneConstraint);
    assertEquals(
        ImmutableSet.of(),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            new ResourceRequest(task, EMPTY)));

    Constraint jvmNegated =
        jvmConstraint.withConstraint(tc -> TaskConstraint.value(tc.getValue().withNegated(true)));
    Constraint zoneNegated =
        zoneConstraint.withConstraint(tc -> TaskConstraint.value(tc.getValue().withNegated(true)));
    assertVetoes(
        makeTask(JOB_A, jvmNegated, zoneNegated),
        hostA,
        Veto.constraintMismatch("jvm"));
  }

  @Test
  public void testVetoScaling() {
    control.replay();

    int maxScore = VetoType.INSUFFICIENT_RESOURCES.getScore();
    assertEquals((int) (maxScore * 1.0 / CPU.getRange()), CPU.veto(1).getScore());
    assertEquals(maxScore, CPU.veto(CPU.getRange() * 10).getScore());
    assertEquals((int) (maxScore * 2.0 / RAM.getRange()), RAM.veto(2).getScore());
    assertEquals((int) (maxScore * 200.0 / DISK.getRange()), DISK.veto(200).getScore());
  }

  @Test
  public void testDuplicatedAttribute() {
    control.replay();

    HostAttributes hostA = hostAttributes(HOST_A,
        valueAttribute("jvm", "1.4"),
        valueAttribute("jvm", "1.6", "1.7"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.4");
    checkConstraint(hostA, "jvm", true, "1.6");
    checkConstraint(hostA, "jvm", true, "1.7");
    checkConstraint(hostA, "jvm", true, "1.6", "1.7");
  }

  @Test
  public void testVetoGroups() {
    control.replay();

    assertEquals(VetoGroup.EMPTY, Veto.identifyGroup(ImmutableSet.of()));

    assertEquals(
        VetoGroup.STATIC,
        Veto.identifyGroup(ImmutableSet.of(
            Veto.constraintMismatch("denied"),
            Veto.insufficientResources("ram", 100),
            Veto.maintenance("draining"))));

    assertEquals(
        VetoGroup.DYNAMIC,
        Veto.identifyGroup(ImmutableSet.of(Veto.unsatisfiedLimit("denied"))));

    assertEquals(
        VetoGroup.MIXED,
        Veto.identifyGroup(ImmutableSet.of(
            Veto.insufficientResources("ram", 100),
            Veto.unsatisfiedLimit("denied"))));
  }

  private TaskConfig checkConstraint(
      HostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(JOB_A, hostAttributes, constraintName, expected, value, vs);
  }

  private TaskConfig checkConstraint(
      JobKey job,
      HostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(
        job,
        EMPTY,
        hostAttributes,
        constraintName,
        expected,
        ValueConstraint.create(false,
            ImmutableSet.<String>builder().add(value).addAll(Arrays.asList(vs)).build()));
  }

  private TaskConfig checkConstraint(
      JobKey job,
      AttributeAggregate aggregate,
      HostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      ValueConstraint value) {

    Constraint constraint = new Constraint(constraintName, TaskConstraint.value(value));
    TaskConfig task = makeTask(job, constraint);
    assertEquals(
        expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(task, aggregate))
            .isEmpty());

    Constraint negated = constraint.deepCopy();
    negated.getConstraint().getValue().setNegated(!value.isNegated());
    TaskConfig negatedTask = makeTask(job, negated);
    assertEquals(
        !expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(negatedTask, aggregate))
            .isEmpty());
    return task;
  }

  private void assertNoVetoes(TaskConfig task, HostAttributes hostAttributes) {
    assertVetoes(task, hostAttributes, EMPTY);
  }

  private void assertNoVetoes(
      TaskConfig task,
      HostAttributes attributes,
      AttributeAggregate jobState) {

    assertVetoes(task, attributes, jobState);
  }

  private void assertVetoes(TaskConfig task, HostAttributes hostAttributes, Veto... vetoes) {
    assertVetoes(task, hostAttributes, EMPTY, vetoes);
  }

  private void assertVetoes(
      TaskConfig task,
      HostAttributes hostAttributes,
      AttributeAggregate jobState,
      Veto... vetoes) {

    assertEquals(
        ImmutableSet.copyOf(vetoes),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(task, jobState)));
  }

  private static HostAttributes hostAttributes(
      String host,
      MaintenanceMode mode,
      Attribute... attributes) {

    return HostAttributes.builder()
        .setHost(host)
        .setMode(mode)
        .setAttributes(attributes)
        .build();
  }

  private static HostAttributes hostAttributes(
      String host,
      Attribute... attributes) {

    return hostAttributes(host, MaintenanceMode.NONE, attributes);
  }

  private Attribute valueAttribute(String name, String string, String... strings) {
    return Attribute.builder().setName(name).setValues(Lists.asList(string, strings)).build();
  }

  private static Constraint makeConstraint(String name, String... values) {
    return Constraint.create(name,
        TaskConstraint.value(ValueConstraint.create(false, ImmutableSet.copyOf(values))));
  }

  private Constraint limitConstraint(String name, int value) {
    return Constraint.create(name, TaskConstraint.limit(LimitConstraint.create(value)));
  }

  private TaskConfig makeTask(JobKey job, Constraint... constraint) {
    return TaskConfig.build(makeTask(job, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setConstraints(Sets.newHashSet(constraint)));
  }

  private TaskConfig hostLimitTask(JobKey job, int maxPerHost) {
    return makeTask(job, limitConstraint(HOST_ATTRIBUTE, maxPerHost));
  }

  private TaskConfig hostLimitTask(int maxPerHost) {
    return hostLimitTask(JOB_A, maxPerHost);
  }

  private TaskConfig rackLimitTask(JobKey job, int maxPerRack) {
    return makeTask(job, limitConstraint(RACK_ATTRIBUTE, maxPerRack));
  }

  private TaskConfig makeTask(JobKey job, int cpus, long ramMb, long diskMb) {
    return TaskConfig.build(new TaskConfig()
        .setJob(job.newBuilder())
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setExecutorConfig(ExecutorConfig.create("aurora", "config"))
        .build();
  }

  private TaskConfig makeTask(int cpus, long ramMb, long diskMb) {
    return makeTask(JOB_A, cpus, ramMb, diskMb);
  }

  private TaskConfig makeTask() {
    return makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
  }
}
