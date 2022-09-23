package com.george.quintas.tradeengine.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.george.quintas.tradeengine.model.*;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

// /////////////////////////////////////////////////
// this file contains all classes related to a trading
// engine which uses a market to simulate a trading platform.
// /////////////////////////////////////////////////
public class TradingEngine {

    private static final Logger LOGGER = LogManager.getLogger("tradingEngine");

    public static interface Listener {
	public void onEvent(EventType type, Object data);
    }

    public static enum EventType {
	SALE, PURCHASE, TIMEOUT_SALESORDER, TIMEOUT_PURCHASEORDER, STATS, STOPPED
    }

    private Market market = new Market();
    private Map<String, MarketPrice> marketPrices = new HashMap<>();
    private Map<String, List<VolumeRecord>> volumeRecords = new HashMap<>();
    private InitialContext ctx = new InitialContext();
    private Map<Seller, List<SalesOrder>> newSalesOrders = new HashMap<>();
    private Map<Buyer, List<PurchaseOrder>> newPurchaseOrders = new HashMap<>();

    private long delay;

    private long timeout;

    private Listener listener;
    private boolean running = true;

    /**
     * if false, then runs in an infinite loop until {@link #stop()} is called.
     * if true, then just runs once, and notifies listener that its stopped when
     * that one run is done, so that the listener can decide when to restart the
     * engine for another trading session.
     */
    private boolean runInActorMode;

    /**
     * basically a buyer goes into the market at a time where they are happy to
     * pay the market price. they take it from the cheapest seller (ie the
     * market price). depending on who is left, the market price goes up or down
     *
     * a trading engine has one market place and it controls the frequency of
     * trades. between trades: - sellers and buyers may enter and exit - all
     * sales are persisted
     * 
     * @param delay
     *            number of milliseconds between trades
     * @param timeout
     *            the number of milliseconds after which incomplete sales or
     *            purchase orders should be removed and their buyer/seller
     *            informed of the (partial) failure.
     * @throws NamingException
     */
    public TradingEngine(long delay, long timeout, Listener listener)
	    throws NamingException {
	this(delay, timeout, listener, false);
    }

    public TradingEngine(long delay, long timeout, Listener listener,
	    boolean runInActorMode) throws NamingException {

	this.delay = delay;
	this.timeout = timeout;
	this.listener = listener;
	this.runInActorMode = runInActorMode;
	LOGGER.debug("market is opening for trading!");
    }

    public void run() {

	while (running) {
	    LOGGER.debug("\n\n------------------------------- trading...-------------------------");
	    long start = System.currentTimeMillis();

	    prepareMarket();

	    List<Sale> sales = market.trade();
	    LOGGER.info("trading completed");

	    noteMarketPricesAndVolumes(sales);

	    try {
		persistSale(sales);
	    } catch (Exception e) {
		LOGGER.error("failed to persist sales: " + sales, e);
	    }
	    LOGGER.info("persisting completed, notifying involved parties...");
	    sales.stream().forEach(sale -> {
		if (sale.getBuyer().listener != null)
		    sale.getBuyer().listener.onEvent(EventType.PURCHASE, sale);
		if (sale.getSeller().listener != null)
		    sale.getSeller().listener.onEvent(EventType.SALE, sale);
	    });
	    if (!sales.isEmpty()) {
		LOGGER.warn("trading of " + sales.size()
			+ " sales completed and persisted in "
			+ (System.currentTimeMillis() - start) + "ms");
	    } else {
		LOGGER.info("no trades...");
	    }

	    // debug(self.market, 10, false);
	    if (listener != null)
		this.updateMarketVolume(null); // removes outdated data
	    listener.onEvent(EventType.STATS,
		    new Object[] { market.getMarketInfo(), this.marketPrices,
			    this.volumeRecords });
	    try {
		Thread.sleep(delay);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }

	    if (runInActorMode) {
		break;
	    }
	}

	listener.onEvent(EventType.STOPPED, null);
    }

    public void stop() {
	this.running = false;
    }

    /**
     * @method @return a VolumeRecord, just with no timestamp. properties are
     *         total in last minute.
     */
    public VolumeRecord getCurrentVolume(String productId) {
	List<VolumeRecord> vrs = this.volumeRecords.get(productId);
	if (vrs != null) {
	    long now = System.currentTimeMillis();
	    vrs = vrs.stream().filter(vr -> {
		return now - vr.timestamp.getTime() < 1000 * 10;
	    }).collect(Collectors.toList()); // remove old
	    this.volumeRecords.put(productId, vrs); // ensure records contains
						    // most up to date

	    // aggregate
	    VolumeRecord vr = new VolumeRecord(productId, 0, 0, null, 0);
	    vr = vrs.stream().reduce(vr, VolumeRecord::add);
	    return vr;
	} else {
	    return new VolumeRecord(productId, 0, 0, null, 0);
	}
    }

    /** @method @return the last known price */
    public MarketPrice getCurrentMarketPrice(String productId) {
	return this.marketPrices.get(productId);
    }

    // handles timed out orders
    private void prepareMarket() {

	// handle timeouted sales orders
	market.getSellers().forEach(
		seller -> {
		    List<SalesOrder> incompleteSOs = seller
			    .removeOutdatedSalesOrders(timeout);
		    incompleteSOs.forEach(so -> {
			if (so.getSeller().listener != null)
			    so.getSeller().listener.onEvent(
				    EventType.TIMEOUT_SALESORDER, so);
			else
			    LOGGER.debug("incomplete SO: " + so);
		    });
		});

	// handle timeouted purchase orders
	market.getBuyers().forEach(
		buyer -> {
		    List<PurchaseOrder> incompletePOs = buyer
			    .removeOutdatedPurchaseOrders(timeout);
		    incompletePOs.forEach(po -> {
			if (po.getBuyer().listener != null)
			    po.getBuyer().listener.onEvent(
				    EventType.TIMEOUT_PURCHASEORDER, po);
			else
			    LOGGER.debug("incomplete PO: " + po);
		    });
		});

	if (!runInActorMode) {
	    // add new SOs and POs
	    synchronized (newSalesOrders) {
		newSalesOrders.forEach((seller, sos) -> {
		    if (!this.market.getSellers().contains(seller)) {
			LOGGER.debug("seller named " + seller.getName()
				+ " doesnt exist -> adding a new one");
			this.market.addSeller(seller);
			seller.listener = listener;
		    } else {
			// swap temp seller with the actual one in the market
			seller = this.market.getSellers().get(
				this.market.getSellers().indexOf(seller));
		    }
		    final Seller fSeller = seller;
		    sos.forEach(so -> {
			fSeller.addSalesOrder(so);
		    });
		});
		newSalesOrders.clear();
	    }

	    synchronized (newPurchaseOrders) {
		newPurchaseOrders.forEach((buyer, pos) -> {
		    if (!this.market.getBuyers().contains(buyer)) {
			LOGGER.debug("buyer named " + buyer.getName()
				+ " doesnt exist -> adding a new one");
			this.market.addBuyer(buyer);
			buyer.listener = listener;
		    } else {
			// swap temp buyer with the actual one in the market
			buyer = this.market.getBuyers().get(
				this.market.getBuyers().indexOf(buyer));
		    }
		    final Buyer fBuyer = buyer;
		    pos.forEach(po -> {
			fBuyer.addPurchaseOrder(po);
		    });
		});
		newPurchaseOrders.clear();
	    }
	}
    }

    private void persistSale(List<Sale> sales) throws Exception {
	if (!sales.isEmpty()) {
	    LOGGER.info("preparing to persist sales");

	    DataSource ds = (DataSource) ctx.lookup("java:comp/env/jdbc/mysql");
	    try (Connection c = ds.getConnection()) {
		PreparedStatement stmt = c
			.prepareStatement("INSERT INTO SALES (BUYER_NAME, SELLER_NAME, PRODUCT_ID, PRICE, QUANTITY, PO_ID, SO_ID) "
				+ "values (?, ?, ?, ?, ?, ?, ?)");
		sales.forEach(sale -> {
		    try {
			int i = 1;
			stmt.setString(i++, sale.getBuyer().getName());
			stmt.setString(i++, sale.getSeller().getName());
			stmt.setInt(i++, Integer.parseInt(sale.getProductId()));
			stmt.setDouble(i++, sale.getPrice());
			stmt.setInt(i++, sale.getQuantity());
			stmt.setInt(i++, sale.getPurchaseOrder().getId());
			stmt.setInt(i++, sale.getSalesOrder().getId());
			if (stmt.execute()) {
			    ResultSet rs = stmt.getGeneratedKeys();
			    rs.next();
			    sale.setId(rs.getInt(1));
			    rs.close();
			}
		    } catch (SQLException e) {
			throw new RuntimeException(e);
		    }
		});
	    }
	}
    }

    private void noteMarketPricesAndVolumes(List<Sale> sales) {
	sales.forEach(sale -> {
	    updateMarketPrice(sale);
	    updateMarketVolume(sale);
	});
    }

    public static class MarketPrice {
	private String productId;
	private double price;
	private Date timestamp;

	public MarketPrice(String productId, double price, Date timestamp) {
	    this.productId = productId;
	    this.price = price;
	    this.timestamp = timestamp;
	}

	public double getPrice() {
	    return price;
	}

	public String getProductId() {
	    return productId;
	}

	public Date getTimestamp() {
	    return timestamp;
	}
    }

    private void updateMarketPrice(Sale sale) {
	MarketPrice mp = marketPrices.get(sale.getProductId());
	if (mp == null
		|| (mp != null && mp.getTimestamp().getTime() < sale
			.getTimestamp().getTime())) {
	    // set price if none is known, or replace price if its older than
	    // current price
	    marketPrices.put(
		    sale.getProductId(),
		    new MarketPrice(sale.getProductId(), sale.getPrice(), sale
			    .getTimestamp()));
	}
    }

    public static class VolumeRecord {
	public static final VolumeRecord EMPTY = new VolumeRecord(null, 0, 0,
		null, 0);
	public String productId;
	public int numberOfSales;
	public double turnover;
	public Date timestamp;
	public int count;

	public VolumeRecord(String productId, int numberOfSales,
		double turnover, Date timestamp, int count) {
	    this.productId = productId;
	    this.numberOfSales = numberOfSales;
	    this.turnover = turnover;
	    this.timestamp = timestamp;
	    this.count = count;
	}

	public static VolumeRecord add(VolumeRecord a, VolumeRecord b) {
	    return new VolumeRecord(b.productId, a.numberOfSales
		    + b.numberOfSales, a.turnover + b.turnover, null, a.count
		    + b.count);
	}

	public static VolumeRecord aggregate(List<VolumeRecord> vrs) {
	    VolumeRecord vr = EMPTY;
	    vr = vrs.stream().reduce(vr, VolumeRecord::add);
	    return vr;

	}
    }

    private void updateMarketVolume(Sale sale) {
	// //////////////
	// remove old ones
	// //////////////
	Map<String, List<VolumeRecord>> newVolumeRecords = new HashMap<>();
	long now = System.currentTimeMillis();
	volumeRecords.forEach((k, v) -> {
	    List<VolumeRecord> vrs = v.stream().filter(vr -> {
		return now - vr.timestamp.getTime() < 1000 * 10;
	    }).collect(Collectors.toList()); // remove older than 10 secs
		newVolumeRecords.put(k, vrs);
	    });
	volumeRecords = newVolumeRecords; // replace the old ones

	// //////////////
	// add new data
	// //////////////
	if (sale != null) {
	    List<VolumeRecord> vrs = volumeRecords.get(sale.getProductId());
	    if (vrs == null) {
		vrs = new ArrayList<>();
	    }
	    vrs.add(new VolumeRecord(sale.getProductId(), sale.getQuantity(),
		    sale.getQuantity() * sale.getPrice(), sale.getTimestamp(),
		    1)); // scale up to "per minute"
	    volumeRecords.put(sale.getProductId(), vrs); // replace with old one
	}
    }

    public PurchaseOrder addPurchaseOrder(String who, String productId,
	    int quantity, int id) {

	if (runInActorMode) {
	    Buyer buyer = new Buyer(who);
	    if (!this.market.getBuyers().contains(buyer)) {
		LOGGER.debug("buyer named " + who
			+ " doesnt exist -> adding a new one");
		this.market.addBuyer(buyer);
		buyer.listener = listener;
	    } else {
		// swap temp buyer with the actual one in the market
		buyer = this.market.getBuyers().get(
			this.market.getBuyers().indexOf(buyer));
	    }
	    PurchaseOrder po = new PurchaseOrder(productId, quantity, 9999.9,
		    id);
	    buyer.addPurchaseOrder(po);
	    return po;
	} else {
	    synchronized (newPurchaseOrders) {
		Buyer b = new Buyer(who);
		List<PurchaseOrder> pos = newPurchaseOrders.get(b);
		if (pos == null) {
		    pos = new ArrayList<>();
		    newPurchaseOrders.put(b, pos);
		}
		PurchaseOrder po = new PurchaseOrder(productId, quantity,
			9999.9, id);
		pos.add(po);
		return po;
	    }

	}
    }

    public SalesOrder addSalesOrder(String who, String productId, int quantity,
	    double price, int id) {

	if (runInActorMode) {
	    Seller seller = new Seller(who);
	    if (!this.market.getSellers().contains(seller)) {
		LOGGER.debug("seller named " + who
			+ " doesnt exist -> adding a new one");
		this.market.addSeller(seller);
		seller.listener = listener;
	    } else {
		// swap temp seller with the actual one in the market
		seller = this.market.getSellers().get(
			this.market.getSellers().indexOf(seller));
	    }
	    SalesOrder so = new SalesOrder(price, productId, quantity, id);
	    seller.addSalesOrder(so);
	    return so;
	} else {
	    synchronized (newSalesOrders) {
		Seller s = new Seller(who);
		List<SalesOrder> sos = newSalesOrders.get(s);
		if (sos == null) {
		    sos = new ArrayList<>();
		    newSalesOrders.put(s, sos);
		}
		SalesOrder so = new SalesOrder(price, productId, quantity, id);
		sos.add(so);
		return so;
	    }
	}
    }

}
