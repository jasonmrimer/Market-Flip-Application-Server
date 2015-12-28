
package com.marketflip.application.shopper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.marketflip.shared.shopper.MF_PricePoint;
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
public class MFA_ShopperManager implements Runnable {

	private BlockingQueue<ArrayList<MF_PricePoint>>					bq;
	private int														completedBQAdditionsCount;
	private int														completedFuturesCount;
	private ArrayList<String>										arrayListOfShoppers;						// TODO turn to shopper TODO get from database
	private ArrayList<ArrayList<MF_PricePoint>>						arrayListOfPricePointsArrayListsPending;
	private ArrayList<ArrayList<MF_PricePoint>>						arrayListOfPricePointsArrayListsInProgress;
	private ExecutorService											executor;
	private ArrayList<Future<MFA_ShopperManager_BQAdditionFuture>>	arrayListOfBQFutures;
	private ArrayList<Future<MFA_ShopperCrawler>>					arrayListOfCrawlerFutures;
	private int														shopperLimit;
	private MF_ShopperDAO											shopperDAO;
	private boolean													isClosed;
	public static final int											MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT	= 6;
	public static final int											MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT	= 2;

	public MFA_ShopperManager() {
		this.arrayListOfBQFutures = new ArrayList<Future<MFA_ShopperManager_BQAdditionFuture>>();
		this.arrayListOfCrawlerFutures = new ArrayList<Future<MFA_ShopperCrawler>>();
		this.arrayListOfPricePointsArrayListsInProgress = new ArrayList<ArrayList<MF_PricePoint>>();
		this.arrayListOfPricePointsArrayListsPending = new ArrayList<ArrayList<MF_PricePoint>>();
		this.arrayListOfShoppers = new ArrayList<String>();
		this.bq = null;
		this.completedFuturesCount = 0;
		this.executor = Executors.newFixedThreadPool(MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT);
		this.shopperDAO = null;
		this.shopperLimit = -1;
		this.isClosed = false;
	}

	public MFA_ShopperManager(BlockingQueue bq) {
		this();
		this.bq = bq;
	}

	public MFA_ShopperManager(BlockingQueue bq, int shopperLimit) {
		this(bq);
		this.shopperLimit = shopperLimit;
	}

	public MFA_ShopperManager(BlockingQueue bq, int shopperLimit, boolean isMock) {
		this(bq, shopperLimit);
		if (isMock) {
			for (int shoppers = 0; shoppers < shopperLimit; shoppers++) {
				this.arrayListOfShoppers.add("shopper" + shoppers);
			}
			this.shopperDAO = new MF_ShopperDAO(true);
		}
		else {
			this.shopperDAO = new MF_ShopperDAO(false);
		}
	}

	public void close() {
		if (isClosed) {
			return;
		}
		else {
			// shutdown to finish all tasks
			executor.shutdown();
			// close and finalize DAO
			shopperDAO.close();
			isClosed = true;
		}
	}

	private void emptyCompletedBlockingQueueFutures() {
		/*
		 * Remove all completed futures
		 */
		// must iterate via FOR because using an iterator causing concurrency errors upon removal of future from arraylist
		for (int index = arrayListOfBQFutures.size() - 1; index > -1; index--) {
			Future<MFA_ShopperManager_BQAdditionFuture> future = arrayListOfBQFutures.get(index);
			if (future.isDone()) {
				try {
					MFA_ShopperManager_BQAdditionFuture completedFuture = future.get();
					ArrayList<MF_PricePoint> pricePointArrayList = completedFuture
							.getPricePointArrayList();
					arrayListOfPricePointsArrayListsInProgress.remove(pricePointArrayList);
					arrayListOfBQFutures.remove(future);
					completedBQAdditionsCount++;
				}
				catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
	}

	private void emptyCompletedCrawlerFutures() {
		/*
		 * Remove all completed futures
		 */
		//		for (Future<MFA_ShopperCrawler> future : arrayListOfCrawlerFutures) {
		// must iterate via FOR because using an iterator causing concurrency errors upon removal of future from arraylist
		for (int index = arrayListOfCrawlerFutures.size() - 1; index > -1; index--) {
			//			if (future.isDone()) {
			if (arrayListOfCrawlerFutures.get(index).isDone()) {
				try {
					MFA_ShopperCrawler completedFuture = arrayListOfCrawlerFutures.get(index).get();
					arrayListOfPricePointsArrayListsPending
							.add(completedFuture.getPricePointsArrayList());
					arrayListOfCrawlerFutures.remove(index);
					completedFuturesCount++;
				}
				catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
	}

	private void fillBlockingQueue() {
		// check if threads available && arrayLists ready for transfer && the blocking can assume more arrayLists
		while (arrayListOfBQFutures.size() < MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT
				&& !arrayListOfPricePointsArrayListsPending.isEmpty()
				&& bq.remainingCapacity() > 0) {
			ArrayList<MF_PricePoint> transferPPAL = arrayListOfPricePointsArrayListsPending
					.remove(0);
			// GET the arrayList to create future for PUTTING to blocking queue 
			arrayListOfBQFutures
					.add(executor.submit(new MFA_ShopperManager_BQAdditionFuture(transferPPAL)));
			// REMOVE arrayList to ensure single future submissions 
			arrayListOfPricePointsArrayListsInProgress.add(transferPPAL);
		}
	}

	private void fillFuturesArray() {
		/*
		 * Fill all empty positions in the arrayListOfCrawlerFutures to reach capacity.
		 */
		// ensure threads available && shoppers available
		while (arrayListOfCrawlerFutures
				.size() < (MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT
						- MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT)
				&& !arrayListOfShoppers.isEmpty()) {
			arrayListOfCrawlerFutures
					.add(executor.submit(new MFA_ShopperCrawler(arrayListOfShoppers.remove(0))));
		}
	}

	@Override
	public void finalize() {
		try {
			close();
		}
		finally {
			try {
				super.finalize();
			}
			catch (Throwable e) {
				System.err.println("Error in MFA_ShopperManager finalize.");
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
		while (completedFuturesCount == -1 || completedFuturesCount < shopperLimit) {
			// ensure always running at capacity
			fillFuturesArray();
			// empty complete futures
			emptyCompletedCrawlerFutures();
			// ensure blockingqueue always full
			fillBlockingQueue();
			// empty complete additions to blocking queue
			emptyCompletedBlockingQueueFutures();
		}

		/*
		 * Complete all additions to the blocking queue before closing down
		 */
		while (completedBQAdditionsCount < completedFuturesCount) {
			// ensure blockingqueue always full
			fillBlockingQueue();
			// empty complete additions to blocking queue
			emptyCompletedBlockingQueueFutures();
		}
		
		// close the object
		close();
	}

	@Override
	public String toString() {
		String toString;
		if (bq == null) {
			toString = "Instance of Market Flip Application Shopping Manager without parameters.";
		}
		else {
			toString = "Instance of Market Flip Application Shopping Manager instantiated with blocking queue size: "
					+ (bq.size() + bq.remainingCapacity()) + ".";
		}
		return toString;
	}

	public ArrayList<String> getArrayListOfShoppers() {
		return arrayListOfShoppers;
	}

	public int getCompletedBlockingQueueAdditions() {
		return completedBQAdditionsCount;
	}

	public int getCompletedFutureCount() {
		return completedFuturesCount;
	}

	public MF_ShopperDAO getShopperDAO() {
		return shopperDAO;
	}

	/**
	 * Use this class to create futures that can check whether complete in order to ensure all
	 * ShopperCrawler results are put into the blocking queue. It acts as a wrapper for the results
	 * of a ShopperCrawler that return as an array list of price points. It wraps via Callable in
	 * order to track its successful addition to the blocking queue to avoid lost data on blocking
	 * queue errors.
	 */
	private class MFA_ShopperManager_BQAdditionFuture
			implements Callable<MFA_ShopperManager_BQAdditionFuture> {

		private ArrayList<MF_PricePoint> pricePointArrayList;

		/**
		 * The purpose of this constructor is to send the result of a Shopper Crawler to the class
		 * and wrap it as a callable to track its future progress.
		 *
		 * @param pricePointArrayList
		 */
		MFA_ShopperManager_BQAdditionFuture(ArrayList<MF_PricePoint> pricePointArrayList) {
			this.pricePointArrayList = pricePointArrayList;
		}

		/**
		 * Returns itself upon completion rather than an arrayList; otherwise, the manager would
		 * only be able to addAll rather than add to futures array lists.
		 * 
		 * @see java.util.concurrent.Callable#call()
		 */
		@Override
		public MFA_ShopperManager_BQAdditionFuture call() throws Exception {
			bq.put(pricePointArrayList);
			return this;
		}

		public ArrayList<MF_PricePoint> getPricePointArrayList() {
			return pricePointArrayList;
		}
	}
}
