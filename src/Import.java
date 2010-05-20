import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

/**
 * @author Matthias Söhnholz
 *
 */
public class Import {

	private Hashtable<String, Integer> idCache = new Hashtable<String,Integer>();
	
	private ADatabase db = new DBDerby();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader sites = null;
		BufferedReader links = null;
		try {
			sites = new BufferedReader(new FileReader("sites"));
			links = new BufferedReader(new FileReader("links"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		Import importer = new Import();
		importer.initDB();

        try {
			while (sites.ready()) {
				String line = sites.readLine();
				String [] site = line.split("<->");
				if (site.length > 1) {
					int status = importer.insertSite(site[0], Integer.parseInt(site[1]));
				}
			}
			importer.flush();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			int site = -1;
			while (links.ready()) {
				String line = links.readLine();
				if (line == null)
					continue;
				if (line.contains("]")) {
					site = Integer.parseInt(line.split("]")[0]);
				}
				else {
					int status = importer.insertLink(site, line);
				}
			}
			importer.flush();
			importer.shutdown();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void initDB() {
		db.init();
	}
	public int insertSite(String title, int id) {
		idCache.put(title, id);
		return db.insertSite(title, id);
	}
	
	public int insertLink(int site, String link) {
		int result = -1;
		int linkId = -1; 
		if (idCache.get(link) != null) {
			linkId = idCache.get(link);
		}
		else
			return result;
		return db.insertLink(site, linkId);
	}
	
	public void flush() {
		db.flush();
	}

	public void shutdown() {
		db.shutdown();
	}
}
