package info.bitrich.xchangestream.bitmex.dto;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum BitmexChannel {
	AFFILIATE("affiliate", true),
	EXECUTION("execution", true),
	ORDER("order", true),
	MARGIN("margin", true),
	POSITION("position", true),
	PRIVATENOTIFICATIONS("privateNotifications", true),
	TRANSACT("transact", true),
	WALLET("wallet", true);

	private final String name;
	private final boolean auth;

	private static Set<BitmexChannel> bitmexPrivateChannels;

	BitmexChannel(String name, boolean auth) {
		this.name = name;
		this.auth = auth;
	}

	public boolean isAuth() {
		return auth;
	}

	public static Set<BitmexChannel> getBitmexPrivateChannels() {
		return Stream.of(values()).filter(BitmexChannel::isAuth).collect(Collectors.toSet());
	}
}
