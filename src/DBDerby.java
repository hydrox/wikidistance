import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class DBDerby {

	private String framework = "embedded";
	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";
	
	private Connection conn = null;

	private PreparedStatement psInsertSite = null;
	private PreparedStatement psInsertLink = null;
	private Statement statement = null;
	
	public void init() {
        Properties props = new Properties(); // connection properties
        // providing a user name and password is optional in the embedded
        // and derbyclient frameworks
//        props.put("user", "wikidist");
//        props.put("password", "wikidist");
        
        String dbName = "wikidistance"; // the name of the database
        
        try {
        	loadDriver();
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
			statement.execute("create index index_sites on sites(ID)");
			System.out.println("Created table sites");
			statement.execute("create table links (site int, link int)");
			statement.execute("create index index_links on links (site)");
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
			System.out.println("Inserted " + id + "\t" + title);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public int insertLink(int site, int link) {
		int result = -1;
		try {
			psInsertLink.setInt(1, site);
			psInsertLink.setInt(2, link);
			result = psInsertSite.executeUpdate();
			System.out.println("Inserted " + site + " -> " + link);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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


