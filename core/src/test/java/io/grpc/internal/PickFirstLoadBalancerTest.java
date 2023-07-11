/*
 * Copyright 2016 The gRPC Authors
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
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;import static org.mockito.Mockito.*;import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.grpc.*;
import io.grpc.LoadBalancer.CreateSubchannelArgs;
import io.grpc.LoadBalancer.Helper;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.LoadBalancer.SubchannelStateListener;
import io.grpc.Status.Code;
import io.grpc.internal.PickFirstLoadBalancer.PickFirstLoadBalancerConfig;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.LinkedList;import java.util.List;import java.util.concurrent.BlockingQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


/** Unit test for {@link PickFirstLoadBalancer}. */
@RunWith(JUnit4.class)
public class PickFirstLoadBalancerTest {
    private PickFirstLoadBalancer loadBalancer;
    private List<EquivalentAddressGroup> servers = Lists.newArrayList();
    private List<SocketAddress> socketAddresses = Lists.newArrayList();
    private List<InternalSubchannel> internalSubchannels = Lists.newArrayList();

    private static final Attributes.Key<String> FOO = Attributes.Key.create("foo");
    private static final String AUTHORITY = "fakeauthority";
    private static final String USER_AGENT = "mosaic";
    private final FakeClock fakeClock = new FakeClock();
    private final InternalChannelz channelz = new InternalChannelz();

    @Mock private BackoffPolicy mockBackoffPolicy1;
    @Mock private BackoffPolicy mockBackoffPolicy2;
    @Mock private BackoffPolicy mockBackoffPolicy3;
    @Mock private BackoffPolicy.Provider mockBackoffPolicyProvider;
    @Mock private ClientTransportFactory mockTransportFactory;

    private final LinkedList<String> callbackInvokes = new LinkedList<>();
    private final InternalSubchannel.Callback mockInternalSubchannelCallback =
            new InternalSubchannel.Callback() {
                @Override
                protected void onTerminated(InternalSubchannel is) {
                    assertSame(InternalSubchannel, is);
                    callbackInvokes.add("onTerminated");
                }

                @Override
                protected void onStateChange(InternalSubchannel is, ConnectivityStateInfo newState) {
                    assertSame(InternalSubchannel, is);
                    callbackInvokes.add("onStateChange:" + newState);
                }

                @Override
                protected void onInUse(InternalSubchannel is) {
                    assertSame(InternalSubchannel, is);
                    callbackInvokes.add("onInUse");
                }

                @Override
                protected void onNotInUse(InternalSubchannel is) {
                    assertSame(InternalSubchannel, is);
                    callbackInvokes.add("onNotInUse");
                }
            };

    private InternalSubchannel InternalSubchannel;
    private BlockingQueue<TestUtils.MockClientTransportInfo> transports;


    private final SynchronizationContext syncContext = new SynchronizationContext(
            new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    throw new AssertionError(e);
                }
            });
    private Attributes affinity = Attributes.newBuilder().set(FOO, "bar").build();
    @Rule
    public final MockitoRule mocks = MockitoJUnit.rule();
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Captor
    private ArgumentCaptor<SubchannelPicker> pickerCaptor;
    @Captor
    private ArgumentCaptor<ConnectivityState> connectivityStateCaptor;
    @Captor
    private ArgumentCaptor<CreateSubchannelArgs> createArgsCaptor;
    @Captor
    private ArgumentCaptor<SubchannelStateListener> stateListenerCaptor;
    @Mock
    private Helper mockHelper;
    @Mock
    private Subchannel mockSubchannel;
    @Mock // This LoadBalancer doesn't use any of the arg fields, as verified in tearDown().
    private PickSubchannelArgs mockArgs;

    @Before
    public void setUp() {
        for (int i = 0; i < 3; i++) {
            SocketAddress addr = new FakeSocketAddress("server" + i);
            servers.add(new EquivalentAddressGroup(addr));
            socketAddresses.add(addr);
        }

        when(mockSubchannel.getAllAddresses()).thenReturn(servers);
        when(mockHelper.getSynchronizationContext()).thenReturn(syncContext);
        when(mockHelper.createSubchannel(any(CreateSubchannelArgs.class))).thenReturn(mockSubchannel);
        when(mockBackoffPolicyProvider.get())
                .thenReturn(mockBackoffPolicy1, mockBackoffPolicy2, mockBackoffPolicy3);
        when(mockBackoffPolicy1.nextBackoffNanos()).thenReturn(10L, 100L);
        when(mockBackoffPolicy2.nextBackoffNanos()).thenReturn(10L, 100L);
        when(mockBackoffPolicy3.nextBackoffNanos()).thenReturn(10L, 100L);
        transports = TestUtils.captureTransports(mockTransportFactory);

        loadBalancer = new PickFirstLoadBalancer(mockHelper);
    }

    @After
    public void tearDown() throws Exception {
        verifyNoMoreInteractions(mockArgs);
    }

    @Test
    public void pickAfterResolved() throws Exception {
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();

        // Calling pickSubchannel() twice gave the same result
        assertEquals(pickerCaptor.getValue().pickSubchannel(mockArgs),
                pickerCaptor.getValue().pickSubchannel(mockArgs));

        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void pickAfterResolved_shuffle() throws Exception {
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity)
                        .setLoadBalancingPolicyConfig(new PickFirstLoadBalancerConfig(true, 123L)).build());

        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        // We should still see the same set of addresses.
        // Because we use a fixed seed, the addresses should always be shuffled in this order.
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);

        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();

        // Calling pickSubchannel() twice gave the same result
        assertEquals(pickerCaptor.getValue().pickSubchannel(mockArgs),
                pickerCaptor.getValue().pickSubchannel(mockArgs));

        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void pickAfterResolved_noShuffle() throws Exception {
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity)
                        .setLoadBalancingPolicyConfig(new PickFirstLoadBalancerConfig(false)).build());

        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();

        // Calling pickSubchannel() twice gave the same result
        assertEquals(pickerCaptor.getValue().pickSubchannel(mockArgs),
                pickerCaptor.getValue().pickSubchannel(mockArgs));

        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void requestConnectionPicker() throws Exception {
        loadBalancer.acceptResolvedAddresses(
            ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());

        InOrder inOrder = inOrder(mockHelper, mockSubchannel);
        inOrder.verify(mockSubchannel).start(stateListenerCaptor.capture());
        SubchannelStateListener stateListener = stateListenerCaptor.getValue();
        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), any(SubchannelPicker.class));
        inOrder.verify(mockSubchannel).requestConnection();

        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(IDLE));
        inOrder.verify(mockHelper).updateBalancingState(eq(IDLE), pickerCaptor.capture());

        SubchannelPicker picker = pickerCaptor.getValue();

        // Calling pickSubchannel() twice gave the same result
        assertEquals(picker.pickSubchannel(mockArgs), picker.pickSubchannel(mockArgs));

        // But the picker calls requestConnection() only once
        inOrder.verify(mockSubchannel).requestConnection();

        verify(mockSubchannel, times(2)).requestConnection();
    }

    @Test
    public void refreshNameResolutionAfterSubchannelConnectionBroken() {
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());

        InOrder inOrder = inOrder(mockHelper, mockSubchannel);
        inOrder.verify(mockSubchannel).start(stateListenerCaptor.capture());
        SubchannelStateListener stateListener = stateListenerCaptor.getValue();
        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        assertSame(mockSubchannel, pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel());
        inOrder.verify(mockSubchannel).requestConnection();

        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(CONNECTING));
        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        assertNull(pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel());
        Status error = Status.UNAUTHENTICATED.withDescription("permission denied");
        stateListener.onSubchannelState(ConnectivityStateInfo.forTransientFailure(error));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());
        assertEquals(error, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());
        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(READY));
        inOrder.verify(mockHelper).updateBalancingState(eq(READY), pickerCaptor.capture());
        assertSame(mockSubchannel, pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel());
        // Simulate receiving go-away so the subchannel transit to IDLE.
        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(IDLE));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(eq(IDLE), any(SubchannelPicker.class));

        verifyNoMoreInteractions(mockHelper, mockSubchannel);
    }

    @Test
    public void pickAfterResolvedAndUnchanged() throws Exception {
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockSubchannel).start(any(SubchannelStateListener.class));
        verify(mockSubchannel).requestConnection();
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
//        verify(mockSubchannel).updateAddresses(eq(servers));
//        verifyNoMoreInteractions(mockSubchannel);

        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        assertThat(createArgsCaptor.getValue()).isNotNull();
        verify(mockHelper)
                .updateBalancingState(isA(ConnectivityState.class), isA(SubchannelPicker.class));
        // Updating the subchannel addresses is unnecessary, but doesn't hurt anything
//        verify(mockHelper).updateAddresses(ArgumentMatchers.<EquivalentAddressGroup>anyList());

//        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void pickAfterResolvedAndChanged() throws Exception {
        SocketAddress socketAddr = new FakeSocketAddress("newserver");
        List<EquivalentAddressGroup> newServers =
                Lists.newArrayList(new EquivalentAddressGroup(socketAddr));

        InOrder inOrder = inOrder(mockHelper, mockSubchannel);

        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        inOrder.verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        verify(mockSubchannel).start(any(SubchannelStateListener.class));
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();
        assertEquals(mockSubchannel, pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel());

        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(newServers).setAttributes(affinity).build());
//        inOrder.verify(mockSubchannel).updateAddresses(eq(newServers)); no longer applicable, we do not update addresses for individual subchannels

//        verifyNoMoreInteractions(mockSubchannel);
//        inOrder.verify(mockHelper).updateAddresses(eq(newServers));
//        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void pickAfterStateChangeAfterResolution() throws Exception {
        InOrder inOrder = inOrder(mockHelper);

        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        inOrder.verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        verify(mockSubchannel).start(stateListenerCaptor.capture());
        SubchannelStateListener stateListener = stateListenerCaptor.getValue();
        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        Subchannel subchannel = pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel();
        reset(mockHelper);
        when(mockHelper.getSynchronizationContext()).thenReturn(syncContext);

        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(IDLE));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(eq(IDLE), pickerCaptor.capture());
        assertEquals(Status.OK, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());

        Status error = Status.UNAVAILABLE.withDescription("boom!");
        stateListener.onSubchannelState(ConnectivityStateInfo.forTransientFailure(error));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());
        assertEquals(error, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());

        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(READY));
        inOrder.verify(mockHelper).updateBalancingState(eq(READY), pickerCaptor.capture());
        assertEquals(subchannel, pickerCaptor.getValue().pickSubchannel(mockArgs).getSubchannel());

        verify(mockHelper, atLeast(0)).getSynchronizationContext();  // Don't care
        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void pickAfterResolutionAfterTransientValue() throws Exception {
        InOrder inOrder = inOrder(mockHelper);

        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        verify(mockSubchannel).start(stateListenerCaptor.capture());
        SubchannelStateListener stateListener = stateListenerCaptor.getValue();
        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();
        reset(mockHelper);
        when(mockHelper.getSynchronizationContext()).thenReturn(syncContext);

        // An error has happened.
        Status error = Status.UNAVAILABLE.withDescription("boom!");
        stateListener.onSubchannelState(ConnectivityStateInfo.forTransientFailure(error));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());
        assertEquals(error, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());

        // But a subsequent IDLE update should be ignored and the LB state not updated. Additionally,
        // a request for a new connection should be made keep the subchannel trying to connect.
        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(IDLE));
        inOrder.verify(mockHelper).refreshNameResolution();
        verifyNoMoreInteractions(mockHelper);
        assertEquals(error, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());
        verify(mockSubchannel, times(2)).requestConnection();

        // Transition from TRANSIENT_ERROR to CONNECTING should also be ignored.
        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(CONNECTING));
        verifyNoMoreInteractions(mockHelper);
        assertEquals(error, pickerCaptor.getValue().pickSubchannel(mockArgs).getStatus());
    }

    @Test
    public void nameResolutionError() throws Exception {
        Status error = Status.NOT_FOUND.withDescription("nameResolutionError");
        loadBalancer.handleNameResolutionError(error);
        verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());
        PickResult pickResult = pickerCaptor.getValue().pickSubchannel(mockArgs);
        assertEquals(null, pickResult.getSubchannel());
        assertEquals(error, pickResult.getStatus());
        verify(mockSubchannel, never()).requestConnection();
        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void nameResolutionError_emptyAddressList() throws Exception {
        servers.clear();
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockHelper).updateBalancingState(connectivityStateCaptor.capture(),
                pickerCaptor.capture());
        PickResult pickResult = pickerCaptor.getValue().pickSubchannel(mockArgs);
        assertThat(pickResult.getSubchannel()).isNull();
        assertThat(pickResult.getStatus().getCode()).isEqualTo(Code.UNAVAILABLE);
        assertThat(pickResult.getStatus().getDescription()).contains("returned no usable address");
        verify(mockSubchannel, never()).requestConnection();
        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void nameResolutionSuccessAfterError() throws Exception {
        InOrder inOrder = inOrder(mockHelper);

        loadBalancer.handleNameResolutionError(Status.NOT_FOUND.withDescription("nameResolutionError"));
        inOrder.verify(mockHelper)
                .updateBalancingState(any(ConnectivityState.class), any(SubchannelPicker.class));
        verify(mockSubchannel, never()).requestConnection();

        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        inOrder.verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAttributes()).isEqualTo(Attributes.EMPTY);
        assertThat(argsList.get(1).getAttributes()).isEqualTo(Attributes.EMPTY);
        assertThat(argsList.get(2).getAttributes()).isEqualTo(Attributes.EMPTY);
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);

        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
        verify(mockSubchannel).requestConnection();

        assertEquals(mockSubchannel, pickerCaptor.getValue().pickSubchannel(mockArgs)
                .getSubchannel());

        assertEquals(pickerCaptor.getValue().pickSubchannel(mockArgs),
                pickerCaptor.getValue().pickSubchannel(mockArgs));

        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void nameResolutionErrorWithStateChanges() throws Exception {
        InOrder inOrder = inOrder(mockHelper);
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        verify(mockSubchannel).start(stateListenerCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);

        inOrder.verify(mockHelper).updateBalancingState(eq(CONNECTING), any(SubchannelPicker.class));

        SubchannelStateListener stateListener = stateListenerCaptor.getValue();

        stateListener.onSubchannelState(ConnectivityStateInfo.forTransientFailure(Status.UNAVAILABLE));
        inOrder.verify(mockHelper).refreshNameResolution();
        inOrder.verify(mockHelper).updateBalancingState(
                eq(TRANSIENT_FAILURE), any(SubchannelPicker.class));

        Status error = Status.NOT_FOUND.withDescription("nameResolutionError");
        loadBalancer.handleNameResolutionError(error);
        inOrder.verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());

        PickResult pickResult = pickerCaptor.getValue().pickSubchannel(mockArgs);
        assertEquals(null, pickResult.getSubchannel());
        assertEquals(error, pickResult.getStatus());

        Status error2 = Status.NOT_FOUND.withDescription("nameResolutionError2");
        loadBalancer.handleNameResolutionError(error2);
        inOrder.verify(mockHelper).updateBalancingState(eq(TRANSIENT_FAILURE), pickerCaptor.capture());

        pickResult = pickerCaptor.getValue().pickSubchannel(mockArgs);
        assertEquals(null, pickResult.getSubchannel());
        assertEquals(error2, pickResult.getStatus());

        verifyNoMoreInteractions(mockHelper);
    }

    @Test
    public void requestConnection() {
        loadBalancer.requestConnection();

        verify(mockSubchannel, never()).requestConnection();
        loadBalancer.acceptResolvedAddresses(
                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
        verify(mockSubchannel).requestConnection();

        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
        verify(mockSubchannel).start(stateListenerCaptor.capture());
        List<CreateSubchannelArgs> argsList = createArgsCaptor.getAllValues();
        assertThat(argsList.get(0).getAddresses().get(0)).isEqualTo(servers.get(0));
        assertThat(argsList.get(1).getAddresses().get(0)).isEqualTo(servers.get(1));
        assertThat(argsList.get(2).getAddresses().get(0)).isEqualTo(servers.get(2));
        assertThat(argsList.get(0).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(1).getAddresses().size()).isEqualTo(1);
        assertThat(argsList.get(2).getAddresses().size()).isEqualTo(1);
        SubchannelStateListener stateListener = stateListenerCaptor.getValue();

        stateListener.onSubchannelState(ConnectivityStateInfo.forNonError(IDLE));
        verify(mockHelper).updateBalancingState(eq(IDLE), any(SubchannelPicker.class));

        verify(mockSubchannel).requestConnection();
        loadBalancer.requestConnection();
        verify(mockSubchannel, times(2)).requestConnection();
    }

    @Test
    public void updateAddresses_emptyEagList_throws() {
      loadBalancer.acceptResolvedAddresses(
          ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
      thrown.expect(IllegalArgumentException.class);
      loadBalancer.updateAddresses(Arrays.<EquivalentAddressGroup>asList());
    }

    @Test
    public void updateAddresses_eagListWithNull_throws() {
      loadBalancer.acceptResolvedAddresses(
          ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
      List<EquivalentAddressGroup> eags = Arrays.asList((EquivalentAddressGroup) null);
      thrown.expect(NullPointerException.class);
      loadBalancer.updateAddresses(eags);
    }

    @Test
    public void updateAddresses_disjoint_connecting() {
      assertEquals(IDLE, loadBalancer.getCurrentState());
      loadBalancer.acceptResolvedAddresses(
          ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
      assertEquals(CONNECTING, loadBalancer.getCurrentState());

      SocketAddress socketAddr = new FakeSocketAddress("newserver");
      List<EquivalentAddressGroup> newServers =
          Lists.newArrayList(new EquivalentAddressGroup(socketAddr));

      loadBalancer.acceptResolvedAddresses(
          ResolvedAddresses.newBuilder().setAddresses(newServers).setAttributes(affinity).build());
      assertEquals(CONNECTING, loadBalancer.getCurrentState());

      // force fail connection attempt to first address

//
//        // First address fails
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        verify(mockTransportFactory)
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // Second address connecting
//        verify(mockTransportFactory)
//                .newClientTransport(
//                        eq(addr2),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        assertNoCallbackInvoke();
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // Update addresses
//        InternalSubchannel.updateAddresses(
//                Arrays.asList(new EquivalentAddressGroup(Arrays.asList(addr3, addr4))));
//        assertNoCallbackInvoke();
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // And new addresses chosen immediately
//        verify(transports.poll().transport).shutdown(any(Status.class));
//        assertNoCallbackInvoke();
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertEquals(0, fakeClock.numPendingTasks());
//        verify(mockTransportFactory)
//                .newClientTransport(
//                        eq(addr3),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        verify(mockTransportFactory)
//                .newClientTransport(
//                        eq(addr4),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        verifyNoMoreInteractions(mockTransportFactory);
//
//        fakeClock.forwardNanos(10); // Drain retry, but don't care about result
    }


//    @Test
//    public void twoAddressesReconnect() {
//        loadBalancer.acceptResolvedAddresses(
//                ResolvedAddresses.newBuilder().setAddresses(servers).setAttributes(affinity).build());
//        verify(mockHelper, times(3)).createSubchannel(createArgsCaptor.capture());
//        verify(mockHelper).updateBalancingState(eq(CONNECTING), pickerCaptor.capture());
//        verify(mockSubchannel).requestConnection();
//
//        // Calling pickSubchannel() twice gave the same result
//        assertEquals(pickerCaptor.getValue().pickSubchannel(mockArgs),
//                pickerCaptor.getValue().pickSubchannel(mockArgs));
//
//        verifyNoMoreInteractions(mockHelper);
//
//        SocketAddress addr1 = mock(SocketAddress.class);
//        SocketAddress addr2 = mock(SocketAddress.class);
//        createInternalSubchannel(addr1);
//        assertEquals(IDLE, InternalSubchannel.getState());
//        createInternalSubchannel(addr2);
//
//        // Invocation counters
//        int transportsAddr1 = 0;
//        int transportsAddr2 = 0;
//        int backoff1Consulted = 0;
//        int backoff2Consulted = 0;
//        int backoff3Consulted = 0;
//        int backoffReset = 0;
//
//        // First attempt
//        assertNoCallbackInvoke();
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        verify(mockTransportFactory, times(++transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//
//        // Let this one fail without success
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        // Still in CONNECTING
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertNoCallbackInvoke();
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // Second attempt will start immediately. Still no back-off policy.
//        verify(mockBackoffPolicyProvider, times(backoffReset)).get();
//        verify(mockTransportFactory, times(++transportsAddr2))
//                .newClientTransport(
//                        eq(addr2),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        // Fail this one too
//        assertNoCallbackInvoke();
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        // All addresses have failed. Delayed transport will be in back-off interval.
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
//        // Backoff reset and first back-off interval begins
//        verify(mockBackoffPolicy1, times(++backoff1Consulted)).nextBackoffNanos();
//        verify(mockBackoffPolicyProvider, times(++backoffReset)).get();
//
//        // No reconnect during TRANSIENT_FAILURE even when requested.
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertNoCallbackInvoke();
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//
//        // Third attempt is the first address, thus controlled by the first back-off interval.
//        fakeClock.forwardNanos(9);
//        verify(mockTransportFactory, times(transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        assertNoCallbackInvoke();
//        fakeClock.forwardNanos(1);
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        verify(mockTransportFactory, times(++transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        // Fail this one too
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // Forth attempt will start immediately. Keep back-off policy.
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        verify(mockBackoffPolicyProvider, times(backoffReset)).get();
//        verify(mockTransportFactory, times(++transportsAddr2))
//                .newClientTransport(
//                        eq(addr2),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        // Fail this one too
//        assertNoCallbackInvoke();
//        transports.poll().listener.transportShutdown(Status.RESOURCE_EXHAUSTED);
//        // All addresses have failed again. Delayed transport will be in back-off interval.
//        assertExactCallbackInvokes("onStateChange:" + RESOURCE_EXHAUSTED_STATE);
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        // Second back-off interval begins
//        verify(mockBackoffPolicy1, times(++backoff1Consulted)).nextBackoffNanos();
//        verify(mockBackoffPolicyProvider, times(backoffReset)).get();
//
//        // Fifth attempt for the first address, thus controlled by the second back-off interval.
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        fakeClock.forwardNanos(99);
//        verify(mockTransportFactory, times(transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        assertNoCallbackInvoke();
//        fakeClock.forwardNanos(1);
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        verify(mockTransportFactory, times(++transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        // Let it through
//        assertNoCallbackInvoke();
//        transports.peek().listener.transportReady();
//        assertExactCallbackInvokes("onStateChange:READY");
//        assertEquals(READY, InternalSubchannel.getState());
//
//        assertSame(
//                transports.peek().transport,
//                ((InternalSubchannel.CallTracingTransport) InternalSubchannel.obtainActiveTransport()).delegate());
//        // Then close it.
//        assertNoCallbackInvoke();
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        assertExactCallbackInvokes("onStateChange:IDLE");
//        assertEquals(IDLE, InternalSubchannel.getState());
//
//        // First attempt after a successful connection. Old back-off policy should be ignored, but there
//        // is not yet a need for a new one. Start from the first address.
//        assertNull(InternalSubchannel.obtainActiveTransport());
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        verify(mockBackoffPolicyProvider, times(backoffReset)).get();
//        verify(mockTransportFactory, times(++transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        // Fail the transport
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//
//        // Second attempt will start immediately. Still no new back-off policy.
//        verify(mockBackoffPolicyProvider, times(backoffReset)).get();
//        verify(mockTransportFactory, times(++transportsAddr2))
//                .newClientTransport(
//                        eq(addr2),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        // Fail this one too
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        transports.poll().listener.transportShutdown(Status.UNAVAILABLE);
//        // All addresses have failed. Enter TRANSIENT_FAILURE. Back-off in effect.
//        assertExactCallbackInvokes("onStateChange:" + UNAVAILABLE_STATE);
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        // Back-off reset and first back-off interval begins
//        verify(mockBackoffPolicy2, times(++backoff2Consulted)).nextBackoffNanos();
//        verify(mockBackoffPolicyProvider, times(++backoffReset)).get();
//
//        // Third attempt is the first address, thus controlled by the first back-off interval.
//        fakeClock.forwardNanos(9);
//        verify(mockTransportFactory, times(transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//        assertEquals(TRANSIENT_FAILURE, InternalSubchannel.getState());
//        assertNoCallbackInvoke();
//        fakeClock.forwardNanos(1);
//        assertExactCallbackInvokes("onStateChange:CONNECTING");
//        assertEquals(CONNECTING, InternalSubchannel.getState());
//        verify(mockTransportFactory, times(++transportsAddr1))
//                .newClientTransport(
//                        eq(addr1),
//                        eq(createClientTransportOptions()),
//                        isA(InternalSubchannel.TransportLogger.class));
//
//        // Final checks on invocations on back-off policies
//        verify(mockBackoffPolicy1, times(backoff1Consulted)).nextBackoffNanos();
//        verify(mockBackoffPolicy2, times(backoff2Consulted)).nextBackoffNanos();
//        verify(mockBackoffPolicy3, times(backoff3Consulted)).nextBackoffNanos();
//    }
//
//
    private void createInternalSubchannel(SocketAddress ... addrs) {
        createInternalSubchannel(new EquivalentAddressGroup(Arrays.asList(addrs)));
    }

    private void createInternalSubchannel(EquivalentAddressGroup ... addrs) {
        List<EquivalentAddressGroup> addressGroups = Arrays.asList(addrs);
        InternalLogId logId = InternalLogId.allocate("Subchannel", /*details=*/ AUTHORITY);
        ChannelTracer subchannelTracer = new ChannelTracer(logId, 10,
                fakeClock.getTimeProvider().currentTimeNanos(), "Subchannel");
        InternalSubchannel internalSubchannel = new InternalSubchannel(addressGroups, AUTHORITY, USER_AGENT,
                mockBackoffPolicyProvider, mockTransportFactory, fakeClock.getScheduledExecutorService(),
                fakeClock.getStopwatchSupplier(), syncContext, mockInternalSubchannelCallback,
                channelz, CallTracer.getDefaultFactory().create(),
                subchannelTracer,
                logId,
                new ChannelLoggerImpl(subchannelTracer, fakeClock.getTimeProvider()));
        internalSubchannels.add(internalSubchannel);
    }
//
//    private void assertNoCallbackInvoke() {
//      while (fakeExecutor.runDueTasks() > 0) {}
//      assertEquals(0, callbackInvokes.size());
//    }
//
//    private void assertExactCallbackInvokes(String ... expectedInvokes) {
//      assertEquals(Arrays.asList(expectedInvokes), callbackInvokes);
//      callbackInvokes.clear();
//    }

    private static class FakeSocketAddress extends SocketAddress {
        final String name;

        FakeSocketAddress(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "FakeSocketAddress-" + name;
        }
    }
}