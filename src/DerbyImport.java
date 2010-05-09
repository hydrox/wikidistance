import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;


/**
 * @author Matthias Söhnholz
 *
 */
public class DerbyImport {

	private String framework = "embedded";
	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";
	
	private Connection conn = null;

	private PreparedStatement psInsertSite = null;
	private PreparedStatement psInsertLink = null;
	private Statement statement = null;

	private Hashtable<String, Integer> idCache = new Hashtable<String,Integer>();
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
        importer.loadDriver();
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
        Properties props = new Properties(); // connection properties
        // providing a user name and password is optional in the embedded
        // and derbyclient frameworks
//        props.put("user", "wikidist");
//        props.put("password", "wikidist");
        
        String dbName = "wikidistance"; // the name of the database
        
        try {
			conn = DriverManager.getConnection(protocol + dbName
			        + ";create=true", props);

            conn.setAutoCommit(false);

            statement = conn.createStatement();
            try {
    			statement.execute("drop table sites");
    			statement.execute("drop table links");
            } catch (SQLException e) {
    		}
			statement.execute("create table sites (ID int, title varchar(255))");
			System.out.println("Created table sites");

			statement.execute("create table links (site int, link int)");
			System.out.println("Created table links");
			
			psInsertSite = conn.prepareStatement("insert into sites values (?, ?)");
			psInsertLink = conn.prepareStatement("insert into links values (?, ?)");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
        System.out.println("Connected to and created database " + dbName);
	}
	
	public int insertSite(String title, int id) {
		int result = -1;
		try {
			psInsertSite.setInt(1, id);
			psInsertSite.setString(2, title);
			result = psInsertSite.executeUpdate();
			idCache.put(title, id);
			System.out.println("Inserted " + id + "\t" + title);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public int insertLink(int site, String link) {
		int result = -1;
		try {
			int linkId = -1; 
			if (idCache.get(link) != null) {
				linkId = idCache.get(link);
			}
			else
				return result;
			psInsertLink.setInt(1, site);
			psInsertLink.setInt(2, linkId);
			result = psInsertSite.executeUpdate();
			System.out.println("Inserted " + site + " -> " + linkId);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(result);
		}
		return result;
	}
	
	public void flush() {
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
    private void loadDriver() {
        try {
            Class.forName(driver).newInstance();
            System.out.println("Loaded the appropriate driver");
        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } catch (InstantiationException ie) {
            System.err.println(
                        "\nUnable to instantiate the JDBC driver " + driver);
            ie.printStackTrace(System.err);
        } catch (IllegalAccessException iae) {
            System.err.println(
                        "\nNot allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
        }
    }
}
