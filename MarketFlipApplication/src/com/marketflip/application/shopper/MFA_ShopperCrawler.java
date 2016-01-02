package com.marketflip.application.shopper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.marketflip.shared.data.MF_ProductsDAO;
import com.marketflip.shared.products.MF_Price;
import com.marketflip.shared.products.MF_Product;
import com.marketflip.shared.shopper.MF_PricePoint;
import com.marketflip.shared.shopper.MF_Shopper;
import com.marketflip.shared.shopper.MF_ShopperDAO;

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

	private ArrayList<MF_PricePoint>		arrayListOfPricePoints;
	private String							shopperEmail;
	private MF_Shopper						shopper;
	private MF_ShopperDAO					shopperDAO;
	private MF_ProductsDAO					productsDAO;
	private HashMap<MF_Product, MF_Price>	hashMapOfMatches;

	public MFA_ShopperCrawler() {
		this.arrayListOfPricePoints = new ArrayList<MF_PricePoint>();
		this.hashMapOfMatches = new HashMap<MF_Product, MF_Price>();
		this.shopperDAO = new MF_ShopperDAO(false);
		this.productsDAO = new MF_ProductsDAO("testing");
	}

	public MFA_ShopperCrawler(String shopperEmail) {
		// TODO Change to Shopper class when class complete
		this();
		this.shopperEmail = shopperEmail;
	}

	public MFA_ShopperCrawler(MF_Shopper shopper) {
		this(shopper.getEmail());
		this.shopper = shopper;
	}

	// TODO ArrayList<MF_PricePoint> to send back
	@Override
	public MFA_ShopperCrawler call() {
		arrayListOfPricePoints = shopperDAO.getArrayListOfPricePoints(shopper);
		// must iterate downward to avoid simultaneous change errors
		for (int index = arrayListOfPricePoints.size() - 1; index > -1; index--) {
			MF_PricePoint pricePoint = arrayListOfPricePoints.get(index);
			MF_Product product = null;
			System.out.println("shoppercrawler for pricepoint " + pricePoint.toString());
			try {
				product = productsDAO.getProduct(pricePoint.getProductUPC());
				System.out.println("shoppercrawler retrieved product: " + product.getUPC());
			}
			catch (SQLException e) {
				System.err.println("Error in ShopperCrawler getting product from DAO");
				e.printStackTrace();
			}
			if (product != null) {
				// iterate through all product's price list for any matches (i.e., below price point price)
				if (!product.getPrices().isEmpty()) {
					for (MF_Price price : product.getPrices()) {
						System.out.println(
								"iterating " + product.getUPC() + " at: " + price.getPrice());
						if (price.getPrice() < pricePoint.getPrice()) {
							hashMapOfMatches.put(product, price);
						}
					}
				}
				else {
					System.err
							.println("Error in shopperCrawler: product prices arraylist is empty.");
				}

			}
			else {
				System.err.println("Error in ShopperCrawler creating hash table: null product");
			}
			// TODO For all matches, consider removing from shopper's price list
			//			if (isMatch(pricePoint)) {
			//				arrayListOfPricePoints.remove(index); // removing by index is cost less to compute
			//			}
		}
		shopperDAO.close();
		productsDAO.close();
		return this;
	}

	public ArrayList<MF_PricePoint> getPricePointsArrayList() {
		return arrayListOfPricePoints;
	}

	public MF_Shopper getShopper() {
		return shopper;
	}

	public HashMap<MF_Product, MF_Price> getHashMapOfPricePointMatches() {
		return hashMapOfMatches;
	}

}
