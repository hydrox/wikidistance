import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This Class provides the nessesery functions to interact with a SQL database.
 * @author Matthias Söhnholz
 *
 */
public abstract class ADatabase {

	protected Connection conn = null;

	protected PreparedStatement psInsertSite = null;
	protected PreparedStatement psInsertLink = null;
	protected Statement statement = null;
	
	/**
	 * Initilses the database
	 */
	abstract public void init();
	
	/**
	 * Opens the database
	 */
	abstract public void open();

	/**
	 * inserts a site-id and the corresponding site-title into the database.
	 * @param title title of the site
	 * @param id id of the site
	 * @return count of manipuleted rows
	 */
	public int insertSite(String title, int id) {
		int result = -1;
		try {
			psInsertSite.setInt(1, id);
			psInsertSite.setString(2, title);
			result = psInsertSite.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * inserts a link into the database.
	 * @param site site-id the link comes from
	 * @param link site-id the link points to
	 * @return count of manipuleted rows
	 */
	public int insertLink(int site, int link) {
		int result = -1;
		try {
			psInsertLink.setInt(1, site);
			psInsertLink.setInt(2, link);
			result = psInsertLink.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * gets the links for a given array of site-ids
	 * @param sites array of site-ids
	 * @return as of yet ugly array of links due to limitations of querysize
	 */
	public int[][] getLinksForSites(int[] sites) {
		int chunkSize = 10;
		int[][] links_a = new int[(sites.length/chunkSize)+1][];
		int link_a_count = 0;
		for(int i=0;i<sites.length;i=i+chunkSize) {
			StringBuilder query = new StringBuilder("SELECT DISTINCT LINK FROM links WHERE site = ");
			for (int j=i;j<Math.min(i+chunkSize, sites.length);j++) {
				query.append(sites[j]);
				query.append(" OR site = ");
			}
			query.append(sites[0]);
			try {
				statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				ResultSet result = statement.executeQuery(query.toString());
				int j=0;
				result.last();
				links_a[link_a_count] = new int [result.getRow()];
				result.beforeFirst();
				while(result.next()) {
					links_a[link_a_count][j++] = result.getInt("link");
				}
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			link_a_count++;
			System.out.print("            \r\t\t" + Math.min(sites.length, i+chunkSize)*100/sites.length + "%");
		}
		return links_a;
	}

	/**
	 * flushs all upcoming changes to the database.
	 */
	public void flush() {
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * shutsdown the database
	 */
	public void shutdown()
	{
		try
		{
			if (statement != null)
			{
				statement.close();
			}
			if (conn != null)
			{
				//DriverManager.getConnection(dbURL + ";shutdown=true");
				conn.close();
			}
		}
		catch (SQLException sqlExcept)
		{
		}

	}

	protected void loadDriver(String driver) {
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
