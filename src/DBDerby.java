import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class connects to a Derby SQL-database.
 * @author Matthias Söhnholz
 *
 */
public class DBDerby  extends ADatabase{

	private String framework = "embedded";
	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";

	public void init() {
		open();
		try {
			try {
				statement.execute("drop table sites");
				statement.execute("drop table links");
			} catch (SQLException e) {
			}
			statement.execute("create table sites (ID int, title varchar(255))");
			statement.execute("create index index_sites on sites(ID)");
			System.out.println("Created table sites");
			statement.execute("create table links (site int, link int)");
			statement.execute("create index index_links on links (site,link)");
			System.out.println("Created table links");
			
			psInsertSite = conn.prepareStatement("insert into sites values (?, ?)");
			psInsertLink = conn.prepareStatement("insert into links values (?, ?)");

		} catch (SQLException e) {
			e.printStackTrace();
		}

        System.out.println("Created tables");
	}

	public void open() {
		Properties props = new Properties(); // connection properties
		// providing a user name and password is optional in the embedded
		// and derbyclient frameworks
//		props.put("user", "wikidist");
//		props.put("password", "wikidist");

		String dbName = "wikidistance"; // the name of the database

		try {
			loadDriver(driver);
			conn = DriverManager.getConnection(protocol + dbName
					+ ";create=true", props);
			conn.setAutoCommit(false);

			statement = conn.createStatement();

			psInsertSite = conn.prepareStatement("insert into sites values (?, ?)");
			psInsertLink = conn.prepareStatement("insert into links values (?, ?)");

		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("Connected to database " + dbName);
	}
}


