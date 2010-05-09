import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;


/**
 * @author Matthias Söhnholz
 *
 */
public class DerbyImport {

	private Hashtable<String, Integer> idCache = new Hashtable<String,Integer>();
	
	private DBDerby derbyDB = new DBDerby();
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

		DerbyImport importer = new DerbyImport();
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
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void initDB() {
		derbyDB.init();
	}
	public int insertSite(String title, int id) {
		idCache.put(title, id);
		return derbyDB.insertSite(title, id);
	}
	
	public int insertLink(int site, String link) {
		int result = -1;
		int linkId = -1; 
		if (idCache.get(link) != null) {
			linkId = idCache.get(link);
		}
		else
			return result;
		return derbyDB.insertLink(site, linkId);
	}
	
	public void flush() {
		derbyDB.flush();
	}
}
