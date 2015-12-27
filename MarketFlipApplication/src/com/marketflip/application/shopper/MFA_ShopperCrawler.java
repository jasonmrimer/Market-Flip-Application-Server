package com.marketflip.application.shopper;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.marketflip.shared.shopper.MF_PricePoint;

/**
 * The purpose of this class is to examine the cross between a shopper's price points list and the
 * available prices for each product on that list via the Product DB. Upon completion of the entire
 * search for a shopper, the object will return any matches via the blockingQueue to its manager in
 * order to send those matches to an notification system.
 *
 * @author highball
 *
 */
public class MFA_ShopperCrawler implements Callable<MFA_ShopperCrawler> {

	private ArrayList<MF_PricePoint> pricePointsArrayList;

	// TODO ArrayList<MF_PricePoint> to send back
	@Override
	public MFA_ShopperCrawler call() throws Exception {
		// TODO Auto-generated method stub
		return this;
	}

	public ArrayList<MF_PricePoint> getPricePointsArrayList() {
		return pricePointsArrayList;
	}

}
