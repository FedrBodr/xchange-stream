package info.bitrich.xchangestream.bitmex;

import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.apache.commons.lang3.StringUtils;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitmex.BitmexException;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.BitmexOrder;
import org.knowm.xchange.bitmex.dto.trade.BitmexPosition;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NoPermissionException;

/**
 * Created by Lukas Zaoralek on 13.11.17.
 */
public class BitmexManualExample {

    private static Logger LOG;
    private static boolean doWork;
    private static BitmexStreamingExchange exchange;

    static {
        System.setProperty("log4j.configurationFile","C:\\projects\\xchange-stream\\xchange-bitmex\\test\\resources\\log4j2.xml");
        LOG = LoggerFactory.getLogger(BitmexManualExample.class);
    }

    public static void main(String[] args) {
        LOG.error("Tesdt");

        // Far safer than temporarily adding these to code that might get committed to VCS
        String apiKey = System.getProperty("bitfinex-api-key");
        String apiSecret = System.getProperty("bitfinex-api-secret");
        if (StringUtils.isEmpty(apiKey) || StringUtils.isEmpty(apiSecret)) {
            throw new IllegalArgumentException("Supply api details in VM args");
        }

        ExchangeSpecification spec = StreamingExchangeFactory.INSTANCE.createExchange(
                BitmexStreamingExchange.class.getName()).getDefaultExchangeSpecification();
        spec.setApiKey(apiKey);
        spec.setSecretKey(apiSecret);
        exchange = (BitmexStreamingExchange) StreamingExchangeFactory.INSTANCE.createExchange(spec);
        exchange.connect().blockingAwait();

        /*streamingMarketDataService.getOrderBook(xbtUsd).subscribe(orderBook -> {
            if (!orderBook.getAsks().isEmpty()) {
                LOG.info("First ask: {}", orderBook.getAsks().get(0));
            }
            if (!orderBook.getBids().isEmpty()) {
                LOG.info("First bid: {}", orderBook.getBids().get(0));
            }
        }, throwable -> LOG.error("ERROR in getting order book: ", throwable));*/

        /*streamingMarketDataService.getRawTicker(xbtUsd).subscribe(ticker -> {
            LOG.info("TICKER: {}", ticker);
        }, throwable -> LOG.error("ERROR in getting ticker: ", throwable));

        streamingMarketDataService.getTicker(xbtUsd).subscribe(ticker -> {
            LOG.info("TICKER: {}", ticker);
        }, throwable -> LOG.error("ERROR in getting ticker: ", throwable));

        exchange.getStreamingMarketDataService().getTrades(xbtUsd)
                .subscribe(trade -> LOG.info("TRADE: {}", trade),
                        throwable -> LOG.error("ERROR in getting trades: ", throwable));*/

		/*streamingMarketDataService.getRawPosition().subscribe(BitmexManualExample::handlePositionMessage, throwable -> LOG.error("ERROR in getting position: ", throwable));*/
		final BitmexStreamingMarketDataService streamingMarketDataService = (BitmexStreamingMarketDataService) exchange.getStreamingMarketDataService();
		CurrencyPair xbtUsd = CurrencyPair.XBT_USD;

		streamingMarketDataService.getRawOrder().subscribe(BitmexManualExample::handleOrderMessage, BitmexManualExample::handleSomeException);

        doWork = true;
        try {
            while(doWork) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        exchange.disconnect().blockingAwait();
    }

	private static void handleErrorMessage(BitmexException bitmexException) {
		LOG.error("ERROR in getting position: ", bitmexException);
	}

	private static void handleSomeException(Throwable throwable) {
    	if(throwable instanceof NoPermissionException){
    		/* do resubscribe to all private channels or just autentificate ? */
			LOG.error("ERROR in getting order: ", throwable);

			BitmexStreamingMarketDataService streamingMarketDataService = (BitmexStreamingMarketDataService) exchange.getStreamingMarketDataService();
			streamingMarketDataService.getRawOrder().subscribe(BitmexManualExample::handleOrderMessage, BitmexManualExample::handleSomeException);

		}
		LOG.error("ubheandled ERROR in getting order: ", throwable);
	}

	private static void handleOrderMessage(BitmexPrivateOrder bitmexOrder) {
        LOG.info("Order: {}", bitmexOrder);
    }

    /* Trader position listener example */
    private static void handlePositionMessage(BitmexPosition position) {
        LOG.info("position: {}", position);
    }
}
