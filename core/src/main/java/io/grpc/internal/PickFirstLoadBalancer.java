/*
 * Copyright 2015 The gRPC Authors
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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.Status;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * A {@link LoadBalancer} that provides no load-balancing over the addresses from the {@link
 * io.grpc.NameResolver}.  The channel's default behavior is used, which is walking down the address
 * list and sticking to the first that works.
 */
final class PickFirstLoadBalancer extends LoadBalancer {
  private final Helper helper;
  
  /**
   * The index of the address corresponding to pendingTransport/activeTransport, or at beginning if
   * both are null.
   *
   * <p>Note: any {@link Index#updateAddresses(List)} should also update {@link #addressGroups}.
   */
  private final Index addressIndex;

  /**
   * All field must be mutated in the syncContext.
   */
  private final SynchronizationContext syncContext;

  /**
   * A volatile accessor to {@link Index#getAddressGroups()}. There are few methods ({@link
   * #getAddressGroups()} and {@link #toString()} access this value where they supposed to access
   * in the {@link #syncContext}. Ideally {@link Index#getAddressGroups()} can be volatile, so we
   * don't need to maintain this volatile accessor. Although, having this accessor can reduce
   * unnecessary volatile reads while it delivers clearer intention of why .
   */
  private volatile List<EquivalentAddressGroup> addressGroups;

  @Nullable
  private ScheduledHandle reconnectTask;

  private List<Subchannel> subchannels;
  private ConnectivityState currentState = IDLE;

  PickFirstLoadBalancer(Helper helper) {
    this.helper = checkNotNull(helper, "helper");
  }

  @Override
  public boolean acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
    List<EquivalentAddressGroup> servers = resolvedAddresses.getAddresses();
    if (servers.isEmpty()) {
      handleNameResolutionError(Status.UNAVAILABLE.withDescription(
          "NameResolver returned no usable address. addrs=" + resolvedAddresses.getAddresses()
              + ", attrs=" + resolvedAddresses.getAttributes()));
      return false;
    }

    // We can optionally be configured to shuffle the address list. This can help better distribute
    // the load.
    if (resolvedAddresses.getLoadBalancingPolicyConfig() instanceof PickFirstLoadBalancerConfig) {
      PickFirstLoadBalancerConfig config
          = (PickFirstLoadBalancerConfig) resolvedAddresses.getLoadBalancingPolicyConfig();
      if (config.shuffleAddressList != null && config.shuffleAddressList) {
        servers = new ArrayList<EquivalentAddressGroup>(servers);
        Collections.shuffle(servers,
            config.randomSeed != null ? new Random(config.randomSeed) : new Random());
      }
    }

    if (subchannel == null) {
      for (server : servers) {
        for (address : server.getAddresses()) {
          final Subchannel subchannel = helper.createSubchannel(
              CreateSubchannelArgs.newBuilder()
                  .setAddresses(address) // setAddress(server)
                  .build());
          subchannel.start(new SubchannelStateListener() {
              @Override
              public void onSubchannelState(ConnectivityStateInfo stateInfo) {
                processSubchannelState(subchannel, stateInfo);
              }
            });
        }
        subchannels.add(subchannel);

      // The channel state does not get updated when doing name resolving today, so for the moment
      // let LB report CONNECTION and call subchannel.requestConnection() immediately.
      updateBalancingState(CONNECTING, new Picker(PickResult.withSubchannel(subchannel)));
      requestConnection();
    } else {
      updateAddresses(servers);
    }

    return true;
  }

  /** Replaces the existing addresses, avoiding unnecessary reconnects. */
  public void updateAddresses(final List<EquivalentAddressGroup> newAddressGroups) {
    Preconditions.checkNotNull(newAddressGroups, "newAddressGroups");
    checkListHasNoNulls(newAddressGroups, "newAddressGroups contains null entry");
    Preconditions.checkArgument(!newAddressGroups.isEmpty(), "newAddressGroups is empty");
    final List<EquivalentAddressGroup> newImmutableAddressGroups =
        Collections.unmodifiableList(new ArrayList<>(newAddressGroups));

    syncContext.execute(new Runnable() {
      @Override
      public void run() {
        ManagedClientTransport savedTransport = null;
        SocketAddress previousAddress = addressIndex.getCurrentAddress();
        addressIndex.updateGroups(newImmutableAddressGroups);
        addressGroups = newImmutableAddressGroups;
        if (state.getState() == READY || state.getState() == CONNECTING) {
          if (!addressIndex.seekTo(previousAddress)) {
            // Forced to drop the connection
            if (state.getState() == READY) {
              savedTransport = activeTransport;
              activeTransport = null;
              addressIndex.reset();
              gotoNonErrorState(IDLE);
            } else {
              pendingTransport.shutdown(
                  Status.UNAVAILABLE.withDescription(
                    "InternalSubchannel closed pending transport due to address change"));
              pendingTransport = null;
              addressIndex.reset();
              startNewTransport();
            }
          }
        }
        if (savedTransport != null) {
          if (shutdownDueToUpdateTask != null) {
            // Keeping track of multiple shutdown tasks adds complexity, and shouldn't generally be
            // necessary. This transport has probably already had plenty of time.
            shutdownDueToUpdateTransport.shutdown(
                Status.UNAVAILABLE.withDescription(
                    "InternalSubchannel closed transport early due to address change"));
            shutdownDueToUpdateTask.cancel();
            shutdownDueToUpdateTask = null;
            shutdownDueToUpdateTransport = null;
          }
          // Avoid needless RPC failures by delaying the shutdown. See
          // https://github.com/grpc/grpc-java/issues/2562
          shutdownDueToUpdateTransport = savedTransport;
          shutdownDueToUpdateTask = syncContext.schedule(
              new Runnable() {
                @Override public void run() {
                  ManagedClientTransport transport = shutdownDueToUpdateTransport;
                  shutdownDueToUpdateTask = null;
                  shutdownDueToUpdateTransport = null;
                  transport.shutdown(
                      Status.UNAVAILABLE.withDescription(
                          "InternalSubchannel closed transport due to address change"));
                }
              },
              ManagedChannelImpl.SUBCHANNEL_SHUTDOWN_DELAY_SECONDS,
              TimeUnit.SECONDS,
              scheduledExecutor);
        }
      }
    });
  }


  @Override
  public void handleNameResolutionError(Status error) {
    if (subchannel != null) {
      subchannel.shutdown();
      subchannel = null;
    }
    // NB(lukaszx0) Whether we should propagate the error unconditionally is arguable. It's fine
    // for time being.
    updateBalancingState(TRANSIENT_FAILURE, new Picker(PickResult.withError(error)));
  }

  private void processSubchannelState(Subchannel subchannel, ConnectivityStateInfo stateInfo) {
    ConnectivityState newState = stateInfo.getState();
    if (newState == SHUTDOWN) {
      return;
    }
    if (newState == TRANSIENT_FAILURE || newState == IDLE) {
      helper.refreshNameResolution();
    }

    // If we are transitioning from a TRANSIENT_FAILURE to CONNECTING or IDLE we ignore this state
    // transition and still keep the LB in TRANSIENT_FAILURE state. This is referred to as "sticky
    // transient failure". Only a subchannel state change to READY will get the LB out of
    // TRANSIENT_FAILURE. If the state is IDLE we additionally request a new connection so that we
    // keep retrying for a connection.
    if (currentState == TRANSIENT_FAILURE) {
      if (newState == CONNECTING) {
        return;
      } else if (newState == IDLE) {
        requestConnection();
        return;
      }
    }

    SubchannelPicker picker;
    switch (newState) {
      case IDLE:
        picker = new RequestConnectionPicker(subchannel);
        break;
      case CONNECTING:
        // It's safe to use RequestConnectionPicker here, so when coming from IDLE we could leave
        // the current picker in-place. But ignoring the potential optimization is simpler.
        picker = new Picker(PickResult.withNoResult());
        break;
      case READY:
        picker = new Picker(PickResult.withSubchannel(subchannel));
        break;
      case TRANSIENT_FAILURE:
        picker = new Picker(PickResult.withError(stateInfo.getStatus()));
        break;
      default:
        throw new IllegalArgumentException("Unsupported state:" + newState);
    }

    updateBalancingState(newState, picker);
  }

  private void updateBalancingState(ConnectivityState state, SubchannelPicker picker) {
    currentState = state;
    helper.updateBalancingState(state, picker);
  }

  @Override
  public void shutdown() {
    if (subchannel != null) {
      subchannel.shutdown();
    }
  }

  @Override
  public void requestConnection() {
    if (subchannel != null) {
      subchannel.requestConnection();
    }
  }

  public void startNewTransport() {
    syncContext.throwIfNotInThisSynchronizationContext();

    Preconditions.checkState(reconnectTask == null, "Should have no reconnectTask scheduled");

    if (addressIndex.isAtBeginning()) {
      connectingTimer.reset().start();
    }
    HttpConnectProxiedSocketAddress proxiedAddr = null;
    if (address instanceof HttpConnectProxiedSocketAddress) {
      proxiedAddr = (HttpConnectProxiedSocketAddress) address;
      address = proxiedAddr.getTargetAddress();
    }

    Attributes currentEagAttributes = addressIndex.getCurrentEagAttributes();
    String eagChannelAuthority = currentEagAttributes
            .get(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE);
    ClientTransportFactory.ClientTransportOptions options =
        new ClientTransportFactory.ClientTransportOptions()
          .setAuthority(eagChannelAuthority != null ? eagChannelAuthority : authority)
          .setEagAttributes(currentEagAttributes)
          .setUserAgent(userAgent)
          .setHttpConnectProxiedSocketAddress(proxiedAddr);
    TransportLogger transportLogger = new TransportLogger();
    // In case the transport logs in the constructor, use the subchannel logId
    transportLogger.logId = getLogId();
    ConnectionClientTransport transport =
        new CallTracingTransport(
            transportFactory
                .newClientTransport(address, options, transportLogger), callsTracer);
    transportLogger.logId = transport.getLogId();
    channelz.addClientSocket(transport);
    pendingTransport = transport;
    transports.add(transport);
    Runnable runnable = transport.start(new TransportListener(transport));
    if (runnable != null) {
      syncContext.executeLater(runnable);
    }
    channelLogger.log(ChannelLogLevel.INFO, "Started transport {0}", transportLogger.logId);

  }

  /**
   * Only called after all addresses attempted and failed (TRANSIENT_FAILURE).
   * @param status the causal status when the channel begins transition to
   *     TRANSIENT_FAILURE.
   */
  private void scheduleBackoff(final Status status) {
    syncContext.throwIfNotInThisSynchronizationContext();

    class EndOfCurrentBackoff implements Runnable {
      @Override
      public void run() {
        reconnectTask = null;
        channelLogger.log(ChannelLogLevel.INFO, "CONNECTING after backoff");
        gotoNonErrorState(CONNECTING);
        startNewTransport();
      }
    }

    gotoState(ConnectivityStateInfo.forTransientFailure(status));
    if (reconnectPolicy == null) {
      reconnectPolicy = backoffPolicyProvider.get();
    }
    long delayNanos =
        reconnectPolicy.nextBackoffNanos() - connectingTimer.elapsed(TimeUnit.NANOSECONDS);
    channelLogger.log(
        ChannelLogLevel.INFO,
        "TRANSIENT_FAILURE ({0}). Will reconnect after {1} ns",
        printShortStatus(status), delayNanos);
    Preconditions.checkState(reconnectTask == null, "previous reconnectTask is not done");
    reconnectTask = syncContext.schedule(
        new EndOfCurrentBackoff(),
        delayNanos,
        TimeUnit.NANOSECONDS,
        scheduledExecutor);
  }

  /**
   * Immediately attempt to reconnect if the current state is TRANSIENT_FAILURE. Otherwise this
   * method has no effect.
   */
  void resetConnectBackoff() {
    syncContext.execute(new Runnable() {
      @Override
      public void run() {
        if (state.getState() != TRANSIENT_FAILURE) {
          return;
        }
        cancelReconnectTask();
        channelLogger.log(ChannelLogLevel.INFO, "CONNECTING; backoff interrupted");
        gotoNonErrorState(CONNECTING);
        startNewTransport();
      }
    });
  }

  @Override
  public void transportShutdown(final Status s) {
    channelLogger.log(
        ChannelLogLevel.INFO, "{0} SHUTDOWN with {1}", transport.getLogId(), printShortStatus(s));
    shutdownInitiated = true;
    syncContext.execute(new Runnable() {
      @Override
      public void run() {
        if (state.getState() == SHUTDOWN) {
          return;
        }
        if (activeTransport == transport) {
          activeTransport = null;
          addressIndex.reset();
          gotoNonErrorState(IDLE);
        } else if (pendingTransport == transport) {
          Preconditions.checkState(state.getState() == CONNECTING,
              "Expected state is CONNECTING, actual state is %s", state.getState());
          addressIndex.increment();
          // Continue reconnect if there are still addresses to try.
          if (!addressIndex.isValid()) {
            pendingTransport = null;
            addressIndex.reset();
            // Initiate backoff
            // Transition to TRANSIENT_FAILURE
            scheduleBackoff(s);
          } else {
            startNewTransport();
          }
        }
      }
    });
  }

  /** Index as in 'i', the pointer to an entry. Not a "search index." */
  @VisibleForTesting
  static final class Index {
    private List<EquivalentAddressGroup> addressGroups;
    private int groupIndex;
    private int addressIndex;

    public Index(List<EquivalentAddressGroup> groups) {
      this.addressGroups = groups;
    }

    public boolean isValid() {
      // addressIndex will never be invalid
      return groupIndex < addressGroups.size();
    }

    public boolean isAtBeginning() {
      return groupIndex == 0 && addressIndex == 0;
    }

    public void increment() {
      EquivalentAddressGroup group = addressGroups.get(groupIndex);
      addressIndex++;
      if (addressIndex >= group.getAddresses().size()) {
        groupIndex++;
        addressIndex = 0;
      }
    }

    public void reset() {
      groupIndex = 0;
      addressIndex = 0;
    }

    public SocketAddress getCurrentAddress() {
      return addressGroups.get(groupIndex).getAddresses().get(addressIndex);
    }

    public Attributes getCurrentEagAttributes() {
      return addressGroups.get(groupIndex).getAttributes();
    }

    public List<EquivalentAddressGroup> getGroups() {
      return addressGroups;
    }

    /** Update to new groups, resetting the current index. */
    public void updateGroups(List<EquivalentAddressGroup> newGroups) {
      addressGroups = newGroups;
      reset();
    }

    /** Returns false if the needle was not found and the current index was left unchanged. */
    public boolean seekTo(SocketAddress needle) {
      for (int i = 0; i < addressGroups.size(); i++) {
        EquivalentAddressGroup group = addressGroups.get(i);
        int j = group.getAddresses().indexOf(needle);
        if (j == -1) {
          continue;
        }
        this.groupIndex = i;
        this.addressIndex = j;
        return true;
      }
      return false;
    }
  }


  /**
   * No-op picker which doesn't add any custom picking logic. It just passes already known result
   * received in constructor.
   */
  private static final class Picker extends SubchannelPicker {
    private final PickResult result;

    Picker(PickResult result) {
      this.result = checkNotNull(result, "result");
    }

    @Override
    public PickResult pickSubchannel(PickSubchannelArgs args) {
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Picker.class).add("result", result).toString();
    }
  }
  
  /** Picker that requests connection during the first pick, and returns noResult. */
  private final class RequestConnectionPicker extends SubchannelPicker {
    private final Subchannel subchannel;
    private final AtomicBoolean connectionRequested = new AtomicBoolean(false);

    RequestConnectionPicker(Subchannel subchannel) {
      this.subchannel = checkNotNull(subchannel, "subchannel");
    }

    @Override
    public PickResult pickSubchannel(PickSubchannelArgs args) {
      if (connectionRequested.compareAndSet(false, true)) {
        helper.getSynchronizationContext().execute(new Runnable() {
            @Override
            public void run() {
              subchannel.requestConnection();
            }
          });
      }
      return PickResult.withNoResult();
    }
  }

  public static final class PickFirstLoadBalancerConfig {

    @Nullable
    public final Boolean shuffleAddressList;

    // For testing purposes only, not meant to be parsed from a real config.
    @Nullable final Long randomSeed;

    public PickFirstLoadBalancerConfig(@Nullable Boolean shuffleAddressList) {
      this(shuffleAddressList, null);
    }

    PickFirstLoadBalancerConfig(@Nullable Boolean shuffleAddressList, @Nullable Long randomSeed) {
      this.shuffleAddressList = shuffleAddressList;
      this.randomSeed = randomSeed;
    }
  }
}
