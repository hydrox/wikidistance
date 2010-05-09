
/**
 * @author Matthias Söhnholz
 *
 */
public class DerbyDistance {

	private DBDerby derbyDB = new DBDerby();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DerbyDistance distance = new DerbyDistance();
		distance.init();
		int [] test = {21915};
		int [] links = distance.getLinksForSites(test);
		for (int i : links) {
			System.out.println(i);
		}
	}

	public void init() {
		derbyDB.open();
	}
	
	public int[] getLinksForSites(int[] sites) {
		int [] links = derbyDB.getLinksForSites(sites);
		return links;
	}
}
