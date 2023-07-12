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
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.grpc.*;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * A {@link LoadBalancer} that provides no load-balancing over the addresses from the {@link
 * io.grpc.NameResolver}.  The channel's default behavior is used, which is walking down the address
 * list and sticking to the first that works.
 */
final class PickFirstLoadBalancer extends LoadBalancer {
    private final Helper helper;
    private volatile List<EquivalentAddressGroup> addressGroups;
    private volatile List<Subchannel> subchannels = new ArrayList<>(); // does this need to be thread-safe/volatile?
    private volatile int index;
    private volatile ConnectivityState currentState = IDLE;

//    /**
//     * All field must be mutated in the syncContext.
//     */
//    private final SynchronizationContext syncContext;

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
        this.addressGroups = servers;

        if (subchannels.size() == 0) {
            index = 0;
            for (EquivalentAddressGroup server : addressGroups) {
                for (SocketAddress address : server.getAddresses()) {
                    List<EquivalentAddressGroup> addresses = new ArrayList<>();
                    addresses.add(new EquivalentAddressGroup(address));
                    final Subchannel subchannel = helper.createSubchannel(
                            CreateSubchannelArgs.newBuilder()
                                    .setAddresses(addresses) // TODO: confirm send single address in eag in list?
                                    .build());
                    subchannels.add(subchannel);
                }
            }
            // The channel state does not get updated when doing name resolving today, so for the moment
            // let LB report CONNECTION and call subchannel.requestConnection() immediately.
            requestConnection();
        } else {
            updateAddresses(servers);
        }

        return true;
    }

    public void updateAddresses(final List<EquivalentAddressGroup> newAddressGroups) {
      Preconditions.checkNotNull(newAddressGroups, "newAddressGroups");
      checkListHasNoNulls(newAddressGroups, "newAddressGroups contains null entry");
      Preconditions.checkArgument(!newAddressGroups.isEmpty(), "newAddressGroups is empty");
      final List<EquivalentAddressGroup> newImmutableAddressGroups =
        Collections.unmodifiableList(new ArrayList<>(newAddressGroups));
      helper.getSynchronizationContext().execute(new Runnable() {
        @Override
        public void run() {
          SocketAddress previousAddress = subchannels.get(index).getAddresses().getAddresses().get(0);
          index = 0;
          addressGroups = newImmutableAddressGroups;
          if (currentState == READY || currentState == CONNECTING) {
            if (seekTo(newAddressGroups, previousAddress) == -1) {
              // forced to drop the connection
                  if (currentState == READY) {
                    // TODO: new state? how to handle
                  } else {
                    // TODO: shut everything down? aka just shut one down but with happy eyeballs
                    // TODO: we would need to shut everything down
                    shutdown();
                    requestConnection();
                  }
            } else {
              shutdown();
            }
          }
        }
      });
    }
//    syncContext.execute(new Runnable() {
//      @Override
//      public void run() {
//        ManagedClientTransport savedTransport = null;
//        SocketAddress previousAddress = addressIndex.getCurrentAddress();
//        addressIndex.updateGroups(newImmutyncableAddressGroups);
//        addressGroups = newImmutableAddressGroups;
//        if (state.getState() == READY || state.getState() == CONNECTING) {
//          if (!addressIndex.seekTo(previousAddress)) {
//            // Forced to drop the connection
//            if (state.getState() == READY) {
//              savedTransport = activeTransport;
//              activeTransport = null;
//              addressIndex.reset();
//              gotoNonErrorState(IDLE);
//            } else {
//              pendingTransport.shutdown(
//                  Status.UNAVAILABLE.withDescription(
//                    "InternalSubchannel closed pending transport due to address change"));
//              pendingTransport = null;
////              addressIndex.reset();
//              startNewTransport();
//            }
//          }
//        }
//        if (savedTransport != null) {
//          if (shutdownDueToUpdateTask != null) {
//            // Keeping track of multiple shutdown tasks adds complexity, and shouldn't generally be
//            // necessary. This transport has probably already had plenty of time.
//            shutdownDueToUpdateTransport.shutdown(
//                Status.UNAVAILABLE.withDescription(
//                    "InternalSubchannel closed transport early due to address change"));
//            shutdownDueToUpdateTask.cancel();
//            shutdownDueToUpdateTask = null;
//            shutdownDueToUpdateTransport = null;
//          }
//          // Avoid needless RPC failures by delaying the shutdown. See
//          // https://github.com/grpc/grpc-java/issues/2562
//          shutdownDueToUpdateTransport = savedTransport;
//          shutdownDueToUpdateTask = syncContext.schedule(
//              new Runnable() {
//                @Override public void run() {
//                  ManagedClientTransport transport = shutdownDueToUpdateTransport;
//                  shutdownDueToUpdateTask = null;
//                  shutdownDueToUpdateTransport = null;
//                  transport.shutdown(
//                      Status.UNAVAILABLE.withDescription(
//                          "InternalSubchannel closed transport due to address change"));
//                }
//              },
//              ManagedChannelImpl.SUBCHANNEL_SHUTDOWN_DELAY_SECONDS,
//              TimeUnit.SECONDS,
//              scheduledExecutor);
//        }
//      }
//    });
//          for (int i = 0; i < newAddressGroups.size(); i++) {
//            for (int j = 0; j < newAddressGroups.get(i).getAddresses().size(); j++) {
//              if (subchannels. newAddressGroups.get(i).getAddresses().get(j)) {
//
//              }
//            }
//          }
//
//          for (int i = 0; i < subchannels.size(); i++) {
//
//            if ()
//          }
//          For (old address: old address)
//          	If (new addresses contains old address)
//          		Add to new list
//
//
//          For (new address : new addresses)
//                If (new list doesn’t contain new address)
//          		add to new list
//        }
//      }

    @Override
    public void handleNameResolutionError(Status error) {
      for (Subchannel subchannel : subchannels) {
        subchannel.shutdown();
        subchannel = null;
      }
      index = 0;
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
                index = 0;
                requestConnection(); // TODO: next or current, confirm
                return;
            }
        }

        SubchannelPicker picker;
        switch (newState) {
            case IDLE:
                index++;
                picker = new RequestConnectionPicker(subchannel); // TODO: should we request for connection directly?
                break;
            case CONNECTING:
                // It's safe to use RequestConnectionPicker here, so when coming from IDLE we could leave
                // the current picker in-place. But ignoring the potential optimization is simpler.
                picker = new Picker(PickResult.withNoResult());
                break;
            case READY:
                picker = new Picker(PickResult.withSubchannel(subchannel));
                updateBalancingState(READY, picker);
                break;
            case TRANSIENT_FAILURE:
                if (index == subchannels.size() - 1) {
                  picker = new Picker(PickResult.withError(stateInfo.getStatus()));
                  updateBalancingState(TRANSIENT_FAILURE, picker);
                  // scheduleBackoff(s);
                } else {
                  index++;
                  requestConnection();
                  picker = new Picker(PickResult.withSubchannel(subchannel));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported state:" + newState);
        }

    }

    private void updateBalancingState(ConnectivityState state, SubchannelPicker picker) {
        currentState = state;
        helper.updateBalancingState(state, picker);
    }

    @Override
    public void shutdown() {
        if (subchannels != null) {
            for (Subchannel subchannel : subchannels) {
                subchannel.shutdown();
            }
        }
    }

    @Override
    public void requestConnection() { // TODO: only start subchannel if it is new connection
        if (index < subchannels.size() && subchannels.get(index) != null) {
            subchannels.get(index).start(new SubchannelStateListener() {
                @Override
                public void onSubchannelState(ConnectivityStateInfo stateInfo) {
                    processSubchannelState(subchannels.get(index), stateInfo);
                }
            });
            updateBalancingState(CONNECTING,
                new Picker(PickResult.withSubchannel(subchannels.get(index))));
            subchannels.get(index).requestConnection();
        }
    }

    private static void checkListHasNoNulls(List<?> list, String msg) {
      for (Object item : list) {
        Preconditions.checkNotNull(item, msg);
      }
    }

    private static int seekTo(List<EquivalentAddressGroup> newAddressGroups, SocketAddress needle) {
      for (int i = 0; i < newAddressGroups.size(); i++) {
        if (newAddressGroups.get(i).getAddresses().get(0).equals(needle)) {
          return i;
        }
      }
      return -1;
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
                      PickFirstLoadBalancer.super.requestConnection(); // TODO: this cannot be subchannel.requestConnection()
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

    public static final class PickFirstLoadBalancerConfig {

        @Nullable
        public final Boolean shuffleAddressList;

        // For testing purposes only, not meant to be parsed from a real config.
        @Nullable
        final Long randomSeed;

        public PickFirstLoadBalancerConfig(@Nullable Boolean shuffleAddressList) {
            this(shuffleAddressList, null);
        }
        PickFirstLoadBalancerConfig(@Nullable Boolean shuffleAddressList, @Nullable Long randomSeed) {
            this.shuffleAddressList = shuffleAddressList;
            this.randomSeed = randomSeed;
        }
    }
}
