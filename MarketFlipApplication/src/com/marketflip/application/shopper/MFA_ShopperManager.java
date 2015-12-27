
package com.marketflip.application.shopper;

import java.util.concurrent.BlockingQueue;

/**
 * The ShopperManager will create and manage instances of shopper crawlers that comb through the
 * Shopper Database then each shopper's price points comparing each price point to its product in
 * the Product DB attempting to match/better offers for the shopper.
 * 
 * @author highball
 *
 */
public class MFA_ShopperManager {

	private BlockingQueue bq;

	public MFA_ShopperManager() {
		this.bq = null;
	}

	public MFA_ShopperManager(BlockingQueue bq) {
		this();
		this.bq = bq;
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
}
