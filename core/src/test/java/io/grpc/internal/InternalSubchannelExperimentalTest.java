/*
 * Copyright 2023 The gRPC Authors
 *
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

package io.grpc.internal;

import static com.google.common.truth.Truth.assertThat;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import io.grpc.Attributes;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.InternalChannelz;
import io.grpc.InternalLogId;
import io.grpc.InternalWithLogId;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.internal.InternalSubchannelExperimental.CallTracingTransport;
import io.grpc.internal.InternalSubchannelExperimental.TransportLogger;
import io.grpc.internal.TestUtils.MockClientTransportInfo;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Unit tests for {@link InternalSubchannelExperimental}.
 */
@RunWith(JUnit4.class)
public class InternalSubchannelExperimentalTest {
  @Rule
  public final MockitoRule mocks = MockitoJUnit.rule();
  @SuppressWarnings("deprecation") // https://github.com/grpc/grpc-java/issues/7467
  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private static final String AUTHORITY = "fakeauthority";
  private static final String USER_AGENT = "mosaic";
  private static final ConnectivityStateInfo UNAVAILABLE_STATE =
      ConnectivityStateInfo.forTransientFailure(Status.UNAVAILABLE);
  private static final ConnectivityStateInfo RESOURCE_EXHAUSTED_STATE =
      ConnectivityStateInfo.forTransientFailure(Status.RESOURCE_EXHAUSTED);
  private static final Status SHUTDOWN_REASON = Status.UNAVAILABLE.withDescription("for test");

  // For scheduled executor
  private final FakeClock fakeClock = new FakeClock();
  // For syncContext
  private final FakeClock fakeExecutor = new FakeClock();
  private final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new AssertionError(e);
        }
      });

  private final InternalChannelz channelz = new InternalChannelz();

  @Mock private BackoffPolicy mockBackoffPolicy1;
  @Mock private BackoffPolicy mockBackoffPolicy2;
  @Mock private BackoffPolicy mockBackoffPolicy3;
  @Mock private BackoffPolicy.Provider mockBackoffPolicyProvider;
  @Mock private ClientTransportFactory mockTransportFactory;

  private final LinkedList<String> callbackInvokes = new LinkedList<>();
  private final InternalSubchannelExperimental.Callback mockInternalSubchannelExperimentalCallback =
      new InternalSubchannelExperimental.Callback() {
        @Override
        protected void onTerminated(InternalSubchannelExperimental is) {
          assertSame(internalSubchannelExperimental, is);
          callbackInvokes.add("onTerminated");
        }

        @Override
        protected void onStateChange(InternalSubchannelExperimental is,
            ConnectivityStateInfo newState) {
          assertSame(internalSubchannelExperimental, is);
          callbackInvokes.add("onStateChange:" + newState);
        }

        @Override
        protected void onInUse(InternalSubchannelExperimental is) {
          assertSame(internalSubchannelExperimental, is);
          callbackInvokes.add("onInUse");
        }

        @Override
        protected void onNotInUse(InternalSubchannelExperimental is) {
          assertSame(internalSubchannelExperimental, is);
          callbackInvokes.add("onNotInUse");
        }
      };

  private InternalSubchannelExperimental internalSubchannelExperimental;
  private BlockingQueue<MockClientTransportInfo> transports;

  @Before public void setUp() {
    when(mockBackoffPolicyProvider.get())
        .thenReturn(mockBackoffPolicy1, mockBackoffPolicy2, mockBackoffPolicy3);
    when(mockBackoffPolicy1.nextBackoffNanos()).thenReturn(10L, 100L);
    when(mockBackoffPolicy2.nextBackoffNanos()).thenReturn(10L, 100L);
    when(mockBackoffPolicy3.nextBackoffNanos()).thenReturn(10L, 100L);
    transports = TestUtils.captureTransports(mockTransportFactory);
  }

  @After public void noMorePendingTasks() {
    assertEquals(0, fakeClock.numPendingTasks());
    assertEquals(0, fakeExecutor.numPendingTasks());
  }

  @Test public void eagAttribute_propagatesToTransport() {
    SocketAddress addr = new SocketAddress() {};
    Attributes attr = Attributes.newBuilder().set(Attributes.Key.create("some-key"), "1").build();
    createInternalSubchannelExperimental(new EquivalentAddressGroup(Arrays.asList(addr), attr));

    // First attempt
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    verify(mockTransportFactory).newClientTransport(
        eq(addr),
        eq(createClientTransportOptions().setEagAttributes(attr)),
        isA(TransportLogger.class));
  }

  @Test public void eagAuthorityOverride_propagatesToTransport() {
    SocketAddress addr = new SocketAddress() {};
    String overriddenAuthority = "authority-override";
    Attributes attr = Attributes.newBuilder()
        .set(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE, overriddenAuthority).build();
    createInternalSubchannelExperimental(new EquivalentAddressGroup(Arrays.asList(addr), attr));

    // First attempt
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    verify(mockTransportFactory).newClientTransport(
        eq(addr),
        eq(createClientTransportOptions().setAuthority(overriddenAuthority).setEagAttributes(attr)),
        isA(TransportLogger.class));
  }

  @Test public void singleAddressReconnect() {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);
    assertEquals(IDLE, internalSubchannelExperimental.getState());

    // Invocation counters
    int transportsCreated = 0;
    int backoff1Consulted = 0;
    int backoff2Consulted = 0;
    int backoffReset = 0;

    // First attempt
    assertEquals(IDLE, internalSubchannelExperimental.getState());
    assertNoCallbackInvoke();
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));

    // Fail this one. Enter TRANSIENT_FAILURE.
    assertNoCallbackInvoke();
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertEquals(TRANSIENT_FAILURE, internalSubchannelExperimental.getState());
    assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
    // Backoff reset and using first back-off value interval
    verify(mockBackoffPolicy1, times(++backoff1Consulted)).nextBackoffNanos();
    verify(mockBackoffPolicyProvider, times(++backoffReset)).get();
    
    // Second attempt
    // Transport creation doesn't happen until time is due
    fakeClock.forwardNanos(9);
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    verify(mockTransportFactory, times(transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));
    assertEquals(TRANSIENT_FAILURE, internalSubchannelExperimental.getState());

    assertNoCallbackInvoke();
    fakeClock.forwardNanos(1);
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));

    // Fail this one too
    assertNoCallbackInvoke();
    // Here we use a different status from the first failure, and verify that it's passed to
    // the callback.
    transports.poll().listener.transportShutdown(Status.RESOURCE_EXHAUSTED);
    assertEquals(TRANSIENT_FAILURE, internalSubchannelExperimental.getState());
    assertExactCallbackInvokes("onStateChange:" + RESOURCE_EXHAUSTED_STATE);
    // Second back-off interval
    verify(mockBackoffPolicy1, times(++backoff1Consulted)).nextBackoffNanos();
    verify(mockBackoffPolicyProvider, times(backoffReset)).get();

    // Third attempt
    // Transport creation doesn't happen until time is due
    fakeClock.forwardNanos(99);
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    verify(mockTransportFactory, times(transportsCreated))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(TransportLogger.class));
    assertEquals(TRANSIENT_FAILURE, internalSubchannelExperimental.getState());
    assertNoCallbackInvoke();
    fakeClock.forwardNanos(1);
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(TransportLogger.class));
    // Let this one succeed, will enter READY state.
    assertNoCallbackInvoke();
    transports.peek().listener.transportReady();
    assertExactCallbackInvokes("onStateChange:READY");
    assertEquals(READY, internalSubchannelExperimental.getState());
    assertSame(
        transports.peek().transport,
        ((CallTracingTransport) internalSubchannelExperimental.obtainActiveTransport()).delegate());

    // Close the READY transport, will enter IDLE state.
    assertNoCallbackInvoke();
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertEquals(IDLE, internalSubchannelExperimental.getState());
    assertExactCallbackInvokes("onStateChange:IDLE");

    // Back-off is reset, and the next attempt will happen immediately
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockBackoffPolicyProvider, times(backoffReset)).get();
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));

    // Final checks for consultations on back-off policies
    verify(mockBackoffPolicy1, times(backoff1Consulted)).nextBackoffNanos();
    verify(mockBackoffPolicy2, times(backoff2Consulted)).nextBackoffNanos();
  }
  
  @Test
  public void connectIsLazy() {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    // Invocation counters
    int transportsCreated = 0;

    // Won't connect until requested
    verify(mockTransportFactory, times(transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));

    // First attempt
    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));

    // Fail this one
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);

    // Will always reconnect after back-off
    fakeClock.forwardNanos(10);
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));

    // Make this one proceed
    transports.peek().listener.transportReady();
    assertExactCallbackInvokes("onStateChange:READY");
    // Then go-away
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:IDLE");

    // No scheduled tasks that would ever try to reconnect ...
    assertEquals(0, fakeClock.numPendingTasks());
    assertEquals(0, fakeExecutor.numPendingTasks());

    // ... until it's requested.
    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockTransportFactory, times(++transportsCreated))
        .newClientTransport(
            eq(addr),
            eq(createClientTransportOptions()),
            isA(TransportLogger.class));
  }

  @Test
  public void shutdownWhenReady() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.obtainActiveTransport();
    MockClientTransportInfo transportInfo = transports.poll();
    transportInfo.listener.transportReady();
    assertExactCallbackInvokes("onStateChange:CONNECTING", "onStateChange:READY");

    internalSubchannelExperimental.shutdown(SHUTDOWN_REASON);
    verify(transportInfo.transport).shutdown(same(SHUTDOWN_REASON));
    assertExactCallbackInvokes("onStateChange:SHUTDOWN");
    transportInfo.listener.transportShutdown(SHUTDOWN_REASON);

    transportInfo.listener.transportTerminated();
    assertExactCallbackInvokes("onTerminated");
    verify(transportInfo.transport, never()).shutdownNow(any(Status.class));
  }

  @Test
  public void shutdownBeforeTransportCreated() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);
  
    // First transport is created immediately
    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockTransportFactory)
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));
  
    // Fail this one
    MockClientTransportInfo transportInfo = transports.poll();
    transportInfo.listener.transportShutdown(Status.UNAVAILABLE);
    transportInfo.listener.transportTerminated();
  
    // Entering TRANSIENT_FAILURE, waiting for back-off
    assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
  
    // Save the reconnectTask before shutting down
    FakeClock.ScheduledTask reconnectTask = null;
    for (FakeClock.ScheduledTask task : fakeClock.getPendingTasks()) {
      if (task.command.toString().contains("EndOfCurrentBackoff")) {
        assertNull("There shouldn't be more than one reconnectTask", reconnectTask);
        assertFalse(task.isDone());
        reconnectTask = task;
      }
    }
    assertNotNull("There should be at least one reconnectTask", reconnectTask);
  
    // Shut down InternalSubchannel before the transport is created.
    internalSubchannelExperimental.shutdown(SHUTDOWN_REASON);
    assertTrue(reconnectTask.isCancelled());
    // InternalSubchannel terminated promptly.
    assertExactCallbackInvokes("onStateChange:SHUTDOWN", "onTerminated");
  
    // Simulate a race between reconnectTask cancellation and execution -- the task runs anyway.
    // This should not lead to the creation of a new transport.
    reconnectTask.command.run();
  
    // Futher call to obtainActiveTransport() is no-op.
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    assertEquals(SHUTDOWN, internalSubchannelExperimental.getState());
    assertNoCallbackInvoke();
  
    // No more transports will be created.
    fakeClock.forwardNanos(10000);
    assertEquals(SHUTDOWN, internalSubchannelExperimental.getState());
    verifyNoMoreInteractions(mockTransportFactory);
    assertEquals(0, transports.size());
    assertNoCallbackInvoke();
  }

  @Test
  public void shutdownBeforeTransportReady() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    MockClientTransportInfo transportInfo = transports.poll();

    // Shutdown the InternalSubchannelExperimental before the pending transport is ready
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    internalSubchannelExperimental.shutdown(SHUTDOWN_REASON);
    assertExactCallbackInvokes("onStateChange:SHUTDOWN");

    // The transport should've been shut down even though it's not the active transport yet.
    verify(transportInfo.transport).shutdown(same(SHUTDOWN_REASON));
    transportInfo.listener.transportShutdown(Status.UNAVAILABLE);
    assertNoCallbackInvoke();
    transportInfo.listener.transportTerminated();
    assertExactCallbackInvokes("onTerminated");
    assertEquals(SHUTDOWN, internalSubchannelExperimental.getState());
  }

  @Test
  public void shutdownNow() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.obtainActiveTransport();
    MockClientTransportInfo t1 = transports.poll();
    t1.listener.transportReady();
    assertExactCallbackInvokes("onStateChange:CONNECTING", "onStateChange:READY");
    t1.listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:IDLE");

    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    MockClientTransportInfo t2 = transports.poll();

    Status status = Status.UNAVAILABLE.withDescription("Requested");
    internalSubchannelExperimental.shutdownNow(status);

    verify(t1.transport).shutdownNow(same(status));
    verify(t2.transport).shutdownNow(same(status));
    assertExactCallbackInvokes("onStateChange:SHUTDOWN");
  }

  @Test
  public void obtainTransportAfterShutdown() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.shutdown(SHUTDOWN_REASON);
    assertExactCallbackInvokes("onStateChange:SHUTDOWN", "onTerminated");
    assertEquals(SHUTDOWN, internalSubchannelExperimental.getState());
    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    verify(mockTransportFactory, times(0))
        .newClientTransport(
            addr,
            createClientTransportOptions(),
            internalSubchannelExperimental.getChannelLogger());
    assertNoCallbackInvoke();
    assertEquals(SHUTDOWN, internalSubchannelExperimental.getState());
  }

  @Test
  public void logId() {
    createInternalSubchannelExperimental(mock(SocketAddress.class));

    assertNotNull(internalSubchannelExperimental.getLogId());
  }

  @Test
  public void inUseState() {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.obtainActiveTransport();
    MockClientTransportInfo t0 = transports.poll();
    t0.listener.transportReady();
    assertExactCallbackInvokes("onStateChange:CONNECTING", "onStateChange:READY");
    t0.listener.transportInUse(true);
    assertExactCallbackInvokes("onInUse");

    t0.listener.transportInUse(false);
    assertExactCallbackInvokes("onNotInUse");

    t0.listener.transportInUse(true);
    assertExactCallbackInvokes("onInUse");
    t0.listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:IDLE");

    assertNull(internalSubchannelExperimental.obtainActiveTransport());
    MockClientTransportInfo t1 = transports.poll();
    t1.listener.transportReady();
    assertExactCallbackInvokes("onStateChange:CONNECTING", "onStateChange:READY");
    t1.listener.transportInUse(true);
    // InternalSubchannelExperimental is already in-use, thus doesn't call the callback
    assertNoCallbackInvoke();

    t1.listener.transportInUse(false);
    // t0 is still in-use
    assertNoCallbackInvoke();

    t0.listener.transportInUse(false);
    assertExactCallbackInvokes("onNotInUse");
  }

  @Test
  public void transportTerminateWithoutExitingInUse() {
    // An imperfect transport that terminates without going out of in-use.
    // InternalSubchannelExperimental will clear the in-use bit for it.
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);

    internalSubchannelExperimental.obtainActiveTransport();
    MockClientTransportInfo t0 = transports.poll();
    t0.listener.transportReady();
    assertExactCallbackInvokes("onStateChange:CONNECTING", "onStateChange:READY");
    t0.listener.transportInUse(true);
    assertExactCallbackInvokes("onInUse");

    t0.listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:IDLE");
    t0.listener.transportTerminated();
    assertExactCallbackInvokes("onNotInUse");
  }

  @Test
  public void transportStartReturnsRunnable() {
    SocketAddress addr1 = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr1);
    final AtomicInteger runnableInvokes = new AtomicInteger(0);
    Runnable startRunnable = new Runnable() {
      @Override
      public void run() {
        runnableInvokes.incrementAndGet();
      }
    };
    transports = TestUtils.captureTransports(mockTransportFactory, startRunnable);

    assertEquals(0, runnableInvokes.get());
    internalSubchannelExperimental.obtainActiveTransport();
    assertEquals(1, runnableInvokes.get());
    internalSubchannelExperimental.obtainActiveTransport();
    assertEquals(1, runnableInvokes.get());

    MockClientTransportInfo t0 = transports.poll();
    t0.listener.transportShutdown(Status.UNAVAILABLE);
    assertEquals(1, runnableInvokes.get());

    // Address failed, waiting for back-off.
    assertEquals(1, runnableInvokes.get());
    // Run out the back-off period
    fakeClock.forwardNanos(10);
    assertEquals(2, runnableInvokes.get());

    // This test doesn't care about scheduled InternalSubchannel callbacks.  Clear it up so that
    // noMorePendingTasks() won't fail.
    fakeExecutor.runDueTasks();
    assertEquals(2, runnableInvokes.get());
  }

  @Test
  public void resetConnectBackoff() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);
  
    // Move into TRANSIENT_FAILURE to schedule reconnect
    internalSubchannelExperimental.obtainActiveTransport();
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    verify(mockTransportFactory)
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
  
    // Save the reconnectTask
    FakeClock.ScheduledTask reconnectTask = null;
    for (FakeClock.ScheduledTask task : fakeClock.getPendingTasks()) {
      if (task.command.toString().contains("EndOfCurrentBackoff")) {
        assertNull("There shouldn't be more than one reconnectTask", reconnectTask);
        assertFalse(task.isDone());
        reconnectTask = task;
      }
    }
    assertNotNull("There should be at least one reconnectTask", reconnectTask);
  
    internalSubchannelExperimental.resetConnectBackoff();
  
    verify(mockTransportFactory, times(2))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    assertTrue(reconnectTask.isCancelled());
  
    // Simulate a race between cancel and the task scheduler. Should be a no-op.
    reconnectTask.command.run();
    assertNoCallbackInvoke();
    verify(mockTransportFactory, times(2))
        .newClientTransport(
        eq(addr),
        eq(createClientTransportOptions()),
        isA(InternalSubchannelExperimental.TransportLogger.class));
    verify(mockBackoffPolicyProvider, times(1)).get();
  
    // Fail the reconnect attempt to verify that a fresh reconnect policy is generated after
    // invoking resetConnectBackoff()
    transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
    assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
    verify(mockBackoffPolicyProvider, times(2)).get();
    fakeClock.forwardNanos(10);
    assertExactCallbackInvokes("onStateChange:CONNECTING");
    assertEquals(CONNECTING, internalSubchannelExperimental.getState());
  }

  @Test
  public void resetConnectBackoff_noopOnIdleTransport() throws Exception {
    SocketAddress addr = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr);
    assertEquals(IDLE, internalSubchannelExperimental.getState());

    internalSubchannelExperimental.resetConnectBackoff();

    assertNoCallbackInvoke();
  }

  @Test
  public void channelzMembership() throws Exception {
    SocketAddress addr1 = mock(SocketAddress.class);
    createInternalSubchannelExperimental(addr1);
    internalSubchannelExperimental.obtainActiveTransport();

    MockClientTransportInfo t0 = transports.poll();
    t0.listener.transportReady();
    assertTrue(channelz.containsClientSocket(t0.transport.getLogId()));
    t0.listener.transportShutdown(Status.RESOURCE_EXHAUSTED);
    t0.listener.transportTerminated();
    assertFalse(channelz.containsClientSocket(t0.transport.getLogId()));
  }

  @Test
  public void channelzStatContainsTransport() throws Exception {
    SocketAddress addr = new SocketAddress() {};
    assertThat(transports).isEmpty();
    createInternalSubchannelExperimental(addr);
    internalSubchannelExperimental.obtainActiveTransport();

    InternalWithLogId registeredTransport
        = Iterables.getOnlyElement(internalSubchannelExperimental.getStats().get().sockets);
    MockClientTransportInfo actualTransport = Iterables.getOnlyElement(transports);
    assertEquals(actualTransport.transport.getLogId(), registeredTransport.getLogId());
  }

  /** Create ClientTransportOptions. Should not be reused if it may be mutated. */
  private ClientTransportFactory.ClientTransportOptions createClientTransportOptions() {
    return new ClientTransportFactory.ClientTransportOptions()
        .setAuthority(AUTHORITY)
        .setUserAgent(USER_AGENT);
  }

  private void createInternalSubchannelExperimental(SocketAddress ... addrs) {
    createInternalSubchannelExperimental(new EquivalentAddressGroup(Arrays.asList(addrs)));
  }

  private void createInternalSubchannelExperimental(EquivalentAddressGroup ... addrs) {
    List<EquivalentAddressGroup> addressGroups = Arrays.asList(addrs);
    InternalLogId logId = InternalLogId.allocate("Subchannel", /*details=*/ AUTHORITY);
    ChannelTracer subchannelTracer = new ChannelTracer(logId, 10,
        fakeClock.getTimeProvider().currentTimeNanos(), "Subchannel");
    internalSubchannelExperimental =
        new InternalSubchannelExperimental(addressGroups, AUTHORITY, USER_AGENT,
        mockBackoffPolicyProvider, mockTransportFactory, fakeClock.getScheduledExecutorService(),
        fakeClock.getStopwatchSupplier(), syncContext, mockInternalSubchannelExperimentalCallback,
        channelz, CallTracer.getDefaultFactory().create(),
        subchannelTracer,
        logId,
            new ChannelLoggerImpl(subchannelTracer, fakeClock.getTimeProvider()));
  }

  private void assertNoCallbackInvoke() {
    while (fakeExecutor.runDueTasks() > 0) {}
    assertEquals(0, callbackInvokes.size());
  }

  private void assertExactCallbackInvokes(String ... expectedInvokes) {
    assertEquals(Arrays.asList(expectedInvokes), callbackInvokes);
    callbackInvokes.clear();
  }
}