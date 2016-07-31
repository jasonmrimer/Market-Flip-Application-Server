package com.marketflip.application.notification;

import java.util.HashMap;
import java.util.concurrent.Callable;

import com.marketflip.application.shopper.MFA_ShopperCrawler;
import com.marketflip.shared.products.MF_Price;
import com.marketflip.shared.products.MF_Product;

public class MFA_Notification implements Callable {

	private MFA_ShopperCrawler shopperCrawler;

	public MFA_Notification() {
		// TODO Auto-generated constructor stub
	}

	public MFA_Notification(MFA_ShopperCrawler shopperCrawler) {
		this();
		this.shopperCrawler = shopperCrawler;
	}

	@Override
	public MFA_Notification call() throws Exception {
		String notification;
		if (shopperCrawler != null) {
			notification = "Dear " + shopperCrawler.getShopper()
					+ ", the following price point matches are availabe for purchase:";
			HashMap<MF_Product, MF_Price> hashMap = shopperCrawler.getHashMapOfPricePointMatches();
			if (hashMap != null) {
			for (MF_Product product : hashMap.keySet()) {
				MF_Price price = hashMap.get(product);
				notification += "\nProduct: " + product.getName();
				notification += "\nfrom: " + price.getCompany();
				notification += "\nfor: " + price.getPrice();
			}
			}
			else {
				notification = "notification failure due to null hashMap (i.e., no price points for shopper)";
			}
		}
		else {
			notification = "notification failure due to null shopperCrawler";
		}
		System.out.println(notification);
		return this;
	}

}
