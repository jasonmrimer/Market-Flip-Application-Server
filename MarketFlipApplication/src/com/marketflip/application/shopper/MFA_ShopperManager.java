
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

/**
 * The ShopperManager will create and manage instances of shopper crawlers that comb through the
 * Shopper Database then each shopper's price points comparing each price point to its product in
 * the Product DB attempting to match/better offers for the shopper.
 * 
 * Start with hashmap from shopper DB that has shopperID & shopperEmail - use email for notifier to
 * avoid second pull from DB
 * 
 * @author highball
 *         create max_blockingqueue_threads and subtract from max_threads to force sharing between
 *         bq & futures
 *
 */
public class MFA_ShopperManager implements Runnable {

	private BlockingQueue											bq;
	private int														completeFutureCount;
	private int														futuresLimit;
	private ArrayList<ArrayList<MF_PricePoint>>						arrayListOfPricePointsArrayListsPending;
	private ArrayList<ArrayList<MF_PricePoint>>						arrayListOfPricePointsArrayListsInProgress;
	private ArrayList<Future<MFA_ShopperManager_BQAdditionFuture>>	futuresArrayListBQ;
	private ArrayList<Future<MFA_ShopperCrawler>>					futuresArrayListCrawler;
	private ExecutorService											executor;
	private int														blockingQueueAdditionsCount;
	public static final int											MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT	= 6;
	public static final int											MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT	= 2;

	public MFA_ShopperManager() {
		this.bq = null;
		this.completeFutureCount = 0;
		this.executor = Executors.newFixedThreadPool(MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT);
		this.arrayListOfPricePointsArrayListsInProgress = new ArrayList<ArrayList<MF_PricePoint>>();
		this.arrayListOfPricePointsArrayListsPending = new ArrayList<ArrayList<MF_PricePoint>>();
		this.futuresArrayListCrawler = new ArrayList<Future<MFA_ShopperCrawler>>();
		this.futuresArrayListBQ = new ArrayList<Future<MFA_ShopperManager_BQAdditionFuture>>();
		this.futuresLimit = -1;
	}

	public MFA_ShopperManager(BlockingQueue bq) {
		this();
		this.bq = bq;
	}

	public MFA_ShopperManager(BlockingQueue bq, int futuresLimit) {
		this(bq);
		this.futuresLimit = futuresLimit;
	}

	@Override
	public void run() {
		/*
		 * This will run continuously if set to -1 (the no-arg for futureslimit); otherwise, it will
		 * run until it reaches the constructed limit.
		 */
		while (completeFutureCount == -1 || completeFutureCount < futuresLimit) {
			// ensure always running at capacity
			fillFuturesArray();
			// empty complete futures
			emptyCompletedCrawlerFutures();
			// ensure blockingqueue always full
			fillBlockingQueue();
			// empty complete additions to blocking queue
			emptyCompletedBlockingQueueFutures();
		}
	}

	private void emptyCompletedBlockingQueueFutures() {
		/*
		 * Remove all completed futures
		 */
		for (Future<MFA_ShopperManager_BQAdditionFuture> future : futuresArrayListBQ) {
			if (future.isDone()) {
				try {
					MFA_ShopperManager_BQAdditionFuture completedFuture = future.get();
					ArrayList<MF_PricePoint> pricePointArrayList = completedFuture
							.getPricePointArrayList();
					arrayListOfPricePointsArrayListsInProgress.remove(pricePointArrayList);
					futuresArrayListBQ.remove(future);
					blockingQueueAdditionsCount++;
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
		while (futuresArrayListBQ.size() < MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT
				&& !arrayListOfPricePointsArrayListsPending.isEmpty() && bq.remainingCapacity() > 0) {
			// GET the arrayList to create future for PUTTING to blocking queue 
			futuresArrayListBQ.add(executor.submit(new MFA_ShopperManager_BQAdditionFuture(
					arrayListOfPricePointsArrayListsPending.get(0))));
			// REMOVE arrayList to ensure single future submissions 
			arrayListOfPricePointsArrayListsInProgress
					.add(arrayListOfPricePointsArrayListsPending.remove(0));
		}
	}

	private void emptyCompletedCrawlerFutures() {
		/*
		 * Remove all completed futures
		 */
//		for (Future<MFA_ShopperCrawler> future : futuresArrayListCrawler) {
		for (int index = futuresArrayListCrawler.size() - 1; index < 0; index--) {
//			if (future.isDone()) {
			if (futuresArrayListCrawler.get(index).isDone()) {
				try {
					MFA_ShopperCrawler completedFuture = futuresArrayListCrawler.get(index).get();
					arrayListOfPricePointsArrayListsPending
							.add(completedFuture.getPricePointsArrayList());
					futuresArrayListCrawler.remove(index);
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

	private void fillFuturesArray() {
		/*
		 * Fill all empty positions in the futuresArrayListCrawler to reach capacity.
		 */
		// ensure threads available
		while (futuresArrayListCrawler.size() < MFA_SHOPPER_MANAGER_MAX_THREAD_COUNT
				- MFA_SHOPPER_MANAGER_MAX_BQ_THREAD_COUNT) {
			futuresArrayListCrawler.add(executor.submit(new MFA_ShopperCrawler()));
		}
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

	public int getCompletedFutureCount() {
		return completeFutureCount;
	}

	/**
	 * Use this class to create futures that can check whether complete in order to ensure all
	 * ShopperCrawler results are put into the blocking queue.
	 */
	private class MFA_ShopperManager_BQAdditionFuture
			implements Callable<MFA_ShopperManager_BQAdditionFuture> {

		private ArrayList<MF_PricePoint> pricePointArrayList;

		MFA_ShopperManager_BQAdditionFuture(ArrayList<MF_PricePoint> pricePointArrayList) {
			this.pricePointArrayList = pricePointArrayList;
		}

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
