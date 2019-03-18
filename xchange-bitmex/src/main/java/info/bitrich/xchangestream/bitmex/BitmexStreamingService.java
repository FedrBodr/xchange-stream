package info.bitrich.xchangestream.bitmex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import info.bitrich.xchangestream.bitmex.dto.BitmexChannel;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.reactivex.Completable;
import org.apache.commons.lang3.StringUtils;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitmex.dto.trade.BitmexOrder;
import org.knowm.xchange.bitmex.service.BitmexDigest;
import org.knowm.xchange.utils.nonce.ExpirationTimeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import info.bitrich.xchangestream.bitmex.dto.BitmexWebSocketSubscriptionMessage;
import info.bitrich.xchangestream.bitmex.dto.BitmexWebSocketTransaction;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import io.reactivex.Observable;
import si.mazi.rescu.SynchronizedValueFactory;

import javax.naming.NoPermissionException;

/**
 * Created by Lukas Zaoralek on 13.11.17.
 */
public class BitmexStreamingService extends JsonNettyStreamingService {
    private static final Logger LOG = LoggerFactory.getLogger(BitmexStreamingService.class);
    private String apiKey;
    private String apiSecret;

    public BitmexStreamingService(String apiUrl) {
        super(apiUrl, Integer.MAX_VALUE);
    }

    @Override
    protected void handleMessage(JsonNode message) {
        if (message.has("info")) {
            String info = message.get("info").asText();
            if("Welcome to the BitMEX Realtime API.".equals(info) && StringUtils.isNoneEmpty(apiKey)){
                authenticate();
            }
            return;
        }
        if(message.has("info") || message.has("success")){
            return;
        }
        if (message.has("error")) {
            String error = message.get("error").asText();

            if(StringUtils.equals("User requested an account-locked subscription but no authorization was provided.", error)){
                super.handleError(message, new NoPermissionException(error));
                return;
            }

            LOG.error("Error with message: " + error);
            return;
        }

        super.handleMessage(message);
    }

    @Override
    protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() {
        return null;
    }

    public Observable<BitmexWebSocketTransaction> subscribeBitmexChannel(String channelName) {
        if(BitmexChannel.getBitmexPrivateChannels().contains(channelName)){
            authenticate();
        }
        return subscribeChannel(channelName).map(s -> {
            BitmexWebSocketTransaction transaction = objectMapper.treeToValue(s, BitmexWebSocketTransaction.class);
            return transaction;
        })
                .share();
    }

    /*@Override
    public String getSubscriptionUniqueId(String channelName, Object... args) {
        if (args.length > 0) {
            return channelName + "-" + args[0].toString();
        } else if(channelName.equals("position")){
            return channelName;
        } else {
            return channelName;
        }
    }*/

    @Override
    protected String getChannelNameFromMessage(JsonNode message) throws IOException {
        if(message.has("error")){
            return message.get("request").get("args").get(0).asText();
        }

        String instrument = message.get("data").get(0).get("symbol").asText();
        String table = message.get("table").asText();
        if("position".equals(table)){
            return table;
        }
        if("order".equals(table)){
            return table;
        }
        return String.format("%s:%s", table, instrument);
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        if(BitmexChannel.getBitmexPrivateChannelsNames().contains(channelName)){
            authenticate();
        }
        BitmexWebSocketSubscriptionMessage subscribeMessage = new BitmexWebSocketSubscriptionMessage("subscribe", new String[]{channelName});
        return objectMapper.writeValueAsString(subscribeMessage);
    }

    @Override
    public String getUnsubscribeMessage(String channelName) throws IOException {
        BitmexWebSocketSubscriptionMessage subscribeMessage = new BitmexWebSocketSubscriptionMessage("unsubscribe", new String[]{});
        return objectMapper.writeValueAsString(subscribeMessage);
    }

    /* Authenticate */
    public void authenticate(){
        try {
            sendMessage(getAuthenticateMessage());
        } catch (IOException e) {
            LOG.error("Exception occurred while auth msg send", e);
        }
    }

    private String getAuthenticateMessage() throws IOException {
//        connect().blockingAwait();

        BitmexDigest bitmexDigest = BitmexDigest.createInstance(apiSecret, apiKey );
        if (bitmexDigest == null)
            return null;

        SynchronizedValueFactory<Long> nonceFactory = new ExpirationTimeFactory(30);

        long nonce = nonceFactory.createValue();
        String payload = "GET/realtime" + nonce;
        String digestString = bitmexDigest.digestString(payload);

        BitmexWebSocketSubscriptionMessage subscribeMessage =
                new BitmexWebSocketSubscriptionMessage("authKeyExpires",
                new Object[]{apiKey, nonce, digestString});

        //sendMessage( );
        return objectMapper.writeValueAsString(subscribeMessage);
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public void setApiSecret(String apiSecret) {
        this.apiSecret = apiSecret;
    }
}
