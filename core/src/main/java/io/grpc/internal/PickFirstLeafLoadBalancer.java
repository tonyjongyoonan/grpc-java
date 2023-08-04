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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ExperimentalApi;
import io.grpc.LoadBalancer;
import io.grpc.Status;import org.omg.CORBA.TRANSIENT;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * A {@link LoadBalancer} that provides no load-balancing over the addresses from the {@link
 * io.grpc.NameResolver}. The channel's default behavior is used, which is walking down the address
 * list and sticking to the first that works.
 */
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/10383")
final class PickFirstLeafLoadBalancer extends LoadBalancer {
  private final Helper helper;
  private final Map<SocketAddress, Subchannel> subchannels = new HashMap<>();
  private final Map<Subchannel, ConnectivityState> states = new HashMap<>();
  private Index addressIndex;
  private volatile ConnectivityState currentState = IDLE;

  PickFirstLeafLoadBalancer(Helper helper) {
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
    for (EquivalentAddressGroup eag : servers) {
      if (eag == null) {
        handleNameResolutionError(Status.UNAVAILABLE.withDescription(
            "NameResolver returned address list with null endpoint. addrs="
            + resolvedAddresses.getAddresses() + ", attrs=" + resolvedAddresses.getAttributes()));
        return false;
      }
    }
    // We can optionally be configured to shuffle the address list. This can help better distribute
    // the load.
    if (resolvedAddresses.getLoadBalancingPolicyConfig()
        instanceof PickFirstLeafLoadBalancerConfig) {
      PickFirstLeafLoadBalancerConfig config
          = (PickFirstLeafLoadBalancerConfig) resolvedAddresses.getLoadBalancingPolicyConfig();
      if (config.shuffleAddressList != null && config.shuffleAddressList) {
        servers = new ArrayList<EquivalentAddressGroup>(servers);
        Collections.shuffle(servers,
            config.randomSeed != null ? new Random(config.randomSeed) : new Random());
      }
    }

    final List<EquivalentAddressGroup> newImmutableAddressGroups =
        Collections.unmodifiableList(new ArrayList<>(servers));

    if (addressIndex == null) {
      addressIndex = new Index(newImmutableAddressGroups);
    } else if (addressIndex.isValid()) {
      // If a ready connection exists in new address list,
      // keep this connection and don't create new subchannels
      SocketAddress previousAddress = addressIndex.getCurrentAddress();
      addressIndex.updateGroups(newImmutableAddressGroups);
      if (addressIndex.seekTo(previousAddress)
          && states.get(subchannels.get(previousAddress)) == READY) {
        return true;
      }
      addressIndex.reset();
    } else {
      addressIndex.updateGroups(newImmutableAddressGroups);
    }

    // Create subchannels for all new addresses, preserving existing connections
    Set<SocketAddress> oldAddrs = new HashSet<>(subchannels.keySet());
    Set<SocketAddress> newAddrs = new HashSet<>();
    for (EquivalentAddressGroup endpoint : newImmutableAddressGroups) {
      for (SocketAddress addr : endpoint.getAddresses()) {
        newAddrs.add(addr);
        if (!subchannels.containsKey(addr)) {
          createNewSubchannel(addr);
        }
      }
    }

    // remove old subchannels that were not in new address list
    for (SocketAddress oldAddr : oldAddrs) {
      if (!newAddrs.contains(oldAddr)) {
        subchannels.get(oldAddr).shutdown();
        subchannels.remove(oldAddr);
      }
    }

    if (oldAddrs.size() == 0 || currentState == CONNECTING || currentState == READY) {
      // start connection attempt at first address
      updateBalancingState(CONNECTING, new Picker(PickResult.withNoResult()));
      requestConnection();

    } else if (currentState == IDLE) {
      // start connection attempt at first address when requested
      SubchannelPicker picker = new RequestConnectionPicker(this);
      updateBalancingState(IDLE, picker);

    } else if (currentState == TRANSIENT_FAILURE) {
      // start connection attempt at first address
      requestConnection();
    }

    return true;
  }

  @Override
  public void handleNameResolutionError(Status error) {
    for (Subchannel subchannel : subchannels.values()) {
      subchannel.shutdown();
    }
    subchannels.clear();
    // NB(lukaszx0) Whether we should propagate the error unconditionally is arguable. It's fine
    // for time being.
    updateBalancingState(TRANSIENT_FAILURE, new Picker(PickResult.withError(error)));
  }

  void processSubchannelState(Subchannel subchannel, ConnectivityStateInfo stateInfo) {
    ConnectivityState newState = stateInfo.getState();
    // Shutdown channels/previously relevant subchannels can still callback with state updates.
    // To prevent pickers from returning these obselete subchannels, this logic
    // is included to check if the current list of active subchannels includes this subchannel.
    if (!subchannel.equals(subchannels.get(subchannel.getAddresses().getAddresses().get(0)))) {
      return;
    }
    if (newState == SHUTDOWN) {
      return;
    }
    if (newState == IDLE) {
      helper.refreshNameResolution();
    }
    // If we are transitioning from a TRANSIENT_FAILURE to CONNECTING or IDLE we ignore this state
    // transition and still keep the LB in TRANSIENT_FAILURE state. This is referred to as "sticky
    // transient failure". Only a subchannel state change to READY will get the LB out of
    // TRANSIENT_FAILURE. If the state is IDLE we additionally request a new connection so that we
    // keep retrying for a connection.

    // With the new pick first implementation, individual subchannels will have their own backoff
    // on a per-address basis. Thus, iterative requests for connections will not be requested
    // once the first pass through is complete.
    // However, every time there is an address update, we will perform a pass through for the new
    // addresses in the updated list.
    if (currentState == TRANSIENT_FAILURE) {
      if (newState == CONNECTING) {
        // each subchannel is responsible for its own backoff
        return;
      } else if (newState == IDLE) {
        requestConnection();
      }
    }

    states.put(subchannel, newState);
    switch (newState) {
      case IDLE:
        // Shutdown when ready: connect from beginning when prompted
        addressIndex.reset();
        updateBalancingState(IDLE, new RequestConnectionPicker(this));;
        break;
      case CONNECTING:
        // It's safe to use RequestConnectionPicker here, so when coming from IDLE we could leave
        // the current picker in-place. But ignoring the potential optimization is simpler.
        updateBalancingState(CONNECTING, new Picker(PickResult.withNoResult()));
        break;
      case READY:
        updateBalancingState(READY, new Picker(PickResult.withSubchannel(subchannel)));
        shutdownRemaining(subchannel.getAddresses().getAddresses().get(0), subchannel);
        addressIndex.reset();
        break;
      case TRANSIENT_FAILURE:
         // If we are looking at current channel, request a connection if possible
        if (addressIndex.isValid() && subchannels.get(addressIndex.getCurrentAddress()) != null
            && subchannels.get(addressIndex.getCurrentAddress()).equals(subchannel)) {
          addressIndex.increment();
          requestConnection();

          // If no addresses remaining, go into TRANSIENT_FAILURE
          if (!addressIndex.isValid()) {
            helper.refreshNameResolution();
            updateBalancingState(TRANSIENT_FAILURE,
                new Picker(PickResult.withError(stateInfo.getStatus())));
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported state:" + newState);
    }
  }

  private void updateBalancingState(ConnectivityState state, SubchannelPicker picker) {
    if (state != currentState || state == READY || state == TRANSIENT_FAILURE) {
      currentState = state;
      helper.updateBalancingState(state, picker);
    }
  }

  @Override
  public void shutdown() {
    for (Subchannel subchannel : subchannels.values()) {
      subchannel.shutdown();
    }
    subchannels.clear();
    states.clear();
  }

  /**
  * Shuts down remaining subchannels. Called when a subchannel becomes ready, which means
  * that all other subchannels must be shutdown.
  */
  private void shutdownRemaining(SocketAddress activeAddr, Subchannel activeSubchannel) {
    for (Subchannel subchannel : subchannels.values()) {
      if (!subchannel.equals(activeSubchannel)) {
        subchannel.shutdown();
      }
    }
    subchannels.clear();
    subchannels.put(activeAddr, activeSubchannel);
    states.clear();
    states.put(activeSubchannel, READY);
  }

  /**
  * Requests a connection to the next applicable address' subchannel, creating one if necessary
  * If the current channel has already attempted a connection, we attempt a connection
  * to the next address/subchannel in our list.
  */
  @Override
  public void requestConnection() {
    if (subchannels.size() == 0) {
      return;
    }
    if (addressIndex.isValid()) {
      Subchannel subchannel = subchannels.containsKey(addressIndex.getCurrentAddress())
          ? subchannels.get(addressIndex.getCurrentAddress())
          : createNewSubchannel(addressIndex.getCurrentAddress());

      ConnectivityState subchannelState = states.get(subchannel);
      if (subchannelState == IDLE) {
        subchannel.requestConnection();
      } else if (subchannelState != CONNECTING) {
        addressIndex.increment();
        requestConnection();
      }
    }
  }

  private Subchannel createNewSubchannel(SocketAddress addr) {
    final Subchannel subchannel = helper.createSubchannel(
        CreateSubchannelArgs.newBuilder()
        .setAddresses(Lists.newArrayList(
            new EquivalentAddressGroup(addr)))
            .build());
    subchannels.put(addr, subchannel);
    states.put(subchannel, IDLE);
    subchannel.start(new SubchannelStateListener() {
      @Override
      public void onSubchannelState(ConnectivityStateInfo stateInfo) {
        processSubchannelState(subchannel, stateInfo);
      }
    });
    return subchannel;
  }

  @VisibleForTesting
  ConnectivityState getCurrentState() {
    return this.currentState;
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

  /**
   * Picker that requests connection during the first pick, and returns noResult.
   */
  private final class RequestConnectionPicker extends SubchannelPicker {
    private final PickFirstLeafLoadBalancer pickFirstLeafLoadBalancer;
    private final AtomicBoolean connectionRequested = new AtomicBoolean(false);

    RequestConnectionPicker(PickFirstLeafLoadBalancer pickFirstLeafLoadBalancer) {
      this.pickFirstLeafLoadBalancer =
          checkNotNull(pickFirstLeafLoadBalancer, "pickFirstLeafLoadBalancer");
    }

    @Override
    public PickResult pickSubchannel(PickSubchannelArgs args) {
      if (connectionRequested.compareAndSet(false, true)) {
        helper.getSynchronizationContext().execute(new Runnable() {
          @Override
          public void run() {
            pickFirstLeafLoadBalancer.requestConnection();
          }
        });
      }
      return PickResult.withNoResult();
    }
  }

  /**
   * Index as in 'i', the pointer to an entry. Not a "search index."
   */
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

    /**
     * Update to new groups, resetting the current index.
     */
    public void updateGroups(List<EquivalentAddressGroup> newGroups) {
      addressGroups = newGroups;
      reset();
    }

    /**
     * Returns false if the needle was not found and the current index was left unchanged.
     */
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

  public static final class PickFirstLeafLoadBalancerConfig {

    @Nullable
    public final Boolean shuffleAddressList;

    // For testing purposes only, not meant to be parsed from a real config.
    @Nullable
    final Long randomSeed;

    public PickFirstLeafLoadBalancerConfig(@Nullable Boolean shuffleAddressList) {
      this(shuffleAddressList, null);
    }

    PickFirstLeafLoadBalancerConfig(@Nullable Boolean shuffleAddressList,
        @Nullable Long randomSeed) {
      this.shuffleAddressList = shuffleAddressList;
      this.randomSeed = randomSeed;
    }
  }
}
