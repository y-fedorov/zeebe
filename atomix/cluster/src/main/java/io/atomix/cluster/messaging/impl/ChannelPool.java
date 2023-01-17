package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;

public interface ChannelPool {
  CompletableFuture<Channel> getChannel(final Address address, final String messageType);
}
