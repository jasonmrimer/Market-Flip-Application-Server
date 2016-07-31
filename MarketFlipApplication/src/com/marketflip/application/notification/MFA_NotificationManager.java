
package com.marketflip.application.notification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.marketflip.application.shopper.MFA_ShopperCrawler;
import com.marketflip.shared.shopper.MF_PricePoint;
import com.marketflip.shared.shopper.MF_Shopper;
import com.marketflip.shared.shopper.MF_ShopperDAO;

/**
 * The ShopperManager will create and manage instances of shopper crawlers that comb through the
 * Shopper Database then each shopper's price points comparing each price point to its product in
 * the Product DB attempting to match/better offers for the shopper.
 * 
 * Start with hashmap from shopper DB that has shopperID & shopperEmail - use email for notifier to
 * avoid second pull from DB
 * 
 * @author highball
 *
 */

//testing the old setup hb 20160730
public class MFA_NotificationManager implements Runnable {

	private BlockingQueue<MFA_ShopperCrawler>		bqOfShopperCrawlersWithMatches;
	private int										completedFuturesCount;
	private ArrayList<MF_Shopper>					arrayListOfShoppers;						// TODO turn to shopper TODO get from database
	private ExecutorService							executor;
	private ArrayList<Future<MFA_ShopperCrawler>>	arrayListOfNotificationFutures;
	private int										notificationLimit;
	private MF_ShopperDAO							shopperDAO;
	private boolean									isClosed;
	public static final int							MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT	= 6;
	public static final int							MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT	= 2;

	public MFA_NotificationManager() {
		this.arrayListOfNotificationFutures = new ArrayList<Future<MFA_ShopperCrawler>>();
		this.arrayListOfShoppers = new ArrayList<MF_Shopper>();
		this.bqOfShopperCrawlersWithMatches = null;
		this.completedFuturesCount = 0;
		this.executor = Executors.newFixedThreadPool(MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT);
		this.shopperDAO = null;
		this.notificationLimit = -1;
		this.isClosed = false;
	}

	public MFA_NotificationManager(BlockingQueue bq) {
		this();
		this.bqOfShopperCrawlersWithMatches = bq;
	}

	public MFA_NotificationManager(BlockingQueue bq, int notificationLimit) {
		this(bq);
		this.notificationLimit = notificationLimit;
	}

	public MFA_NotificationManager(BlockingQueue bq, int shopperLimit, boolean isMock) {
		this(bq, shopperLimit);
		if (isMock) {
			for (int shoppers = 0; shoppers < shopperLimit; shoppers++) {
				this.arrayListOfShoppers.add(
						new MF_Shopper("shopper" + shoppers, "shopper" + shoppers + "@gmail.com"));
			}
			this.shopperDAO = new MF_ShopperDAO(true);
		}
		else {
			this.shopperDAO = new MF_ShopperDAO(false);
			this.arrayListOfShoppers = shopperDAO.getArrayListOfShoppers();
		}
	}

	public void close() {
		executor.shutdown();
		try {
			super.finalize();
		}
		catch (Throwable e) {
			System.err.println("Error in NotificationManager finalizing in close()");
			e.printStackTrace();
		}
	}

	private void emptyCompletedCrawlerFutures() {
		/*
		 * Remove all completed futures
		 */
		//		for (Future<MFA_ShopperCrawler> future : arrayListOfNotificationFutures) {
		// must iterate via FOR because using an iterator causing concurrency errors upon removal of future from arraylist
		for (int index = arrayListOfNotificationFutures.size() - 1; index > -1; index--) {
			//			if (future.isDone()) {
			if (arrayListOfNotificationFutures.get(index).isDone()) {
				arrayListOfNotificationFutures.remove(index);
				completedFuturesCount++;
			}
		}
	}

	private void fillFuturesArray() {
		/*
		 * Fill all empty positions in the arrayListOfNotificationFutures to reach capacity.
		 */
		// ensure threads available && shoppers available
		while (arrayListOfNotificationFutures
				.size() < (MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT
						- MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT)
				&& !bqOfShopperCrawlersWithMatches.isEmpty()) {
			try {
				System.out.println("notifying for "
						+ bqOfShopperCrawlersWithMatches.peek().getShopper().toString());
				arrayListOfNotificationFutures.add(executor
						.submit(new MFA_Notification(bqOfShopperCrawlersWithMatches.take())));
			}
			catch (InterruptedException e) {
				System.err.println("Error in NotificationManager taking from queue.");
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		/*
		 * This will run continuously if set to -1 (the no-arg for futureslimit); otherwise, it will
		 * run until it reaches the constructed limit.
		 */
		while (completedFuturesCount == -1 || completedFuturesCount < notificationLimit) {
			// ensure always running at capacity
			fillFuturesArray();
			// empty complete futures
			emptyCompletedCrawlerFutures();
		}
		System.out.println("notification manager shutting down after completed count: " + completedFuturesCount);
		close();
	}

	@Override
	public String toString() {
		String toString;
		if (bqOfShopperCrawlersWithMatches == null) {
			toString = "Instance of Market Flip Application Notification Manager without parameters.";
		}
		else {
			toString = "Instance of Market Flip Application Notification Manager instantiated with blocking queue size: "
					+ (bqOfShopperCrawlersWithMatches.size()
							+ bqOfShopperCrawlersWithMatches.remainingCapacity())
					+ ".";
		}
		return toString;
	}

	public int getCompletedFutureCount() {
		return completedFuturesCount;
	}
}
