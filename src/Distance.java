import java.util.HashSet;

/**
 * @author Matthias Söhnholz
 *
 */
public class Distance {

	private ADatabase db = new DBDerby();

	private int[] alreadySeen;
	private int maxDist = -1;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Distance distance = new Distance();
		distance.init();
		int tempMax = -1;
		for (int i=2;i<2000;i++) {
			int temp = distance.findLongestDistance(i);
			tempMax = Math.max(temp, tempMax);
			if (temp > 0)
				System.out.println("maxDist for siteID " + i + ": " + temp + " (global max: " + tempMax + ")");
		}
	}

	public void init() {
		db.open();
	}

	public int[][] getLinksForSites(int[] sites) {
		return db.getLinksForSites(sites);
	}

	public int[][] getLinksForSite(int site) {
		int [] sites = {site};
		int [][] links = db.getLinksForSites(sites);
		return links;
	}

	public int findLongestDistance(int startpoint) {
		maxDist=0;
		alreadySeen = new int [1];
		int [] toSearch = {startpoint};
		HashSet<Integer> tempSeen = new HashSet<Integer>();
		tempSeen.add(startpoint);
		alreadySeen[0] = startpoint;
		
		while (tempSeen.size()>0) {
			maxDist++;
			System.out.print("   \rdepth " + maxDist);

			toSearch = new int[tempSeen.size()];
			int i=0;
			for (Integer integer : tempSeen) {
				toSearch[i++] = integer;
			}
			int[][] tmp = getLinksForSites(toSearch);
			for (int[] js : tmp) {
				if (js != null)
					for (int integer : js) {
						tempSeen.add(integer);
					}
			}
			for (Integer integer : alreadySeen) {
				tempSeen.remove(integer);
			}
			alreadySeen = extendArray(alreadySeen, tempSeen.size());

			i=0;
			for (Integer integer : tempSeen) {
				alreadySeen[i++] = integer;
			}
			System.out.print("\r");
		}
		maxDist--;
		return maxDist;
	}
	
	private int[] extendArray(int[] array, int addedSize) {
		int [] newArray = new int[array.length + addedSize];
		for (int i=0;i<array.length;i++) {
			newArray[i+addedSize] = array[i];
		}
		return newArray;
	}
}
