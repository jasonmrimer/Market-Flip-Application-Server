package com.marketflip.application.main;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.marketflip.application.notification.MFA_NotificationManager;
import com.marketflip.application.shopper.MFA_ShopperCrawler;
import com.marketflip.application.shopper.MFA_ShopperManager;

public class MFA_Main {

	private static ArrayBlockingQueue<MFA_ShopperCrawler> bq;
	private static ExecutorService executor;
	
	public static void main(String[] args) {
		Thread crawlerThread;
		Thread notificationThread;
		executor = Executors.newCachedThreadPool();
		bq = new ArrayBlockingQueue<MFA_ShopperCrawler>(3);
		MFA_ShopperManager shopperManager;
		MFA_NotificationManager notificationManager;
		int shopperLimit, notificationLimit;
		if (args.length == 0) {
			shopperLimit = 0;
			notificationLimit = 0;
			crawlerThread = new Thread(new MFA_ShopperManager(bq, shopperLimit, false));
			notificationThread = new Thread(new MFA_NotificationManager(bq, notificationLimit, false));
		}
		else {
			shopperLimit = Integer.valueOf(args[0]);
			notificationLimit =  Integer.valueOf(args[1]);
			bq = new ArrayBlockingQueue<>(3);
			shopperManager = new MFA_ShopperManager(bq, shopperLimit, false);
			notificationManager = new MFA_NotificationManager(bq, notificationLimit, false);
			crawlerThread = new Thread(shopperManager);
			notificationThread = new Thread(notificationManager);
		}
		executor.execute(crawlerThread);
		executor.execute(notificationThread);
	}
	
	public ExecutorService getExecutor() {
		return executor;
	}
}
