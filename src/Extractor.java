import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Matthias Söhnholz
 *
 */
public class Extractor {

	public final static short ERROR = -1;
	public final static short INIT = 0;
	public final static short NAMESPACES = 1;
	public final static short PAGE = 2;
	public final static short TITLE = 3;
	public final static short ID = 4;
	public final static short REVISION = 5;
	public final static short TEXT = 6;

	private static final String P_NAMESPACES = "<namespaces>";
	private static final String P_END_NAMESPACES = "</namespaces>";
	private static final String P_PAGE = "<page>";
	private static final String P_END_PAGE = "</page>";
	private static final String P_REVISION = "<revision>";
	private static final String P_END_REVISION = "</revision>";
	private static final String P_TEXT = "<text";
	private static final String P_END_TEXT = "</text>";
	
	private static final Pattern M_NAMESPACE = Pattern.compile(".*<namespace.*>(.*)</namespace>");
	private static final Pattern M_TITLE = Pattern.compile(".*<title>(.*)</title>");
	private static final Pattern M_ID = Pattern.compile(".*<id>(.*)</id>");
	private static final Pattern M_TEXT = Pattern.compile(".*<text.*>.*");
	private static final Pattern M_LINK = Pattern.compile("\\[\\[([^\\[\\]\\|]*)\\]\\]");
	
	private HashSet<String> namespaces = new HashSet<String>();

	private short status = INIT;
	
	private String title = null;
	private int id = -1;
	
	private BufferedWriter sites = null;
	private BufferedWriter links = null;

	public Extractor() {
		try {
			sites = new BufferedWriter(new FileWriter("sites"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			links = new BufferedWriter(new FileWriter("links"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param input next line to parse 
	 * @return Statuscode showing the status of the parser
	 */
	public short parseLine(String input) {
		if (input==null) {
			return ERROR;
		}
		if (status == INIT) {
			if (input.contains(P_NAMESPACES)) {
				status = NAMESPACES;
			}
			else if (input.contains(P_PAGE)) {
				status = PAGE;
			}
		}
		else if (status == NAMESPACES) {
			if (input.contains(P_END_NAMESPACES)) {
				status = INIT;
			}
			else {
				Matcher m = M_NAMESPACE.matcher(input);
				if (m.matches()) {
					namespaces.add(m.group(1));
				}
			}
		}
		else if (status == PAGE) {
			if (input.contains(P_REVISION)) {
				status = REVISION;
			}
			else if (input.contains(P_END_PAGE)) {
				try {
					sites.write(title + "<->" + id + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
				title = null;
				id = -1;
				status = INIT;
			}
			else if (input.contains(P_END_TEXT)) {
				System.exit(status);
				status = TEXT;
			}
			else {
				Matcher m = M_TITLE.matcher(input);
				if (m.matches()) {
					title = m.group(1);
				}
				m = M_ID.matcher(input);
				if (m.matches()) {
					id = Integer.parseInt(m.group(1));
				}
			}
		}
		else if (status == REVISION || status == TEXT) {
			if (input.contains(P_END_REVISION)) {
				status = PAGE;
			}
			if (M_TEXT.matcher(input).matches()) {
				status = TEXT;
				try {
					links.write(id + "]\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (status == TEXT) {
				Matcher m = M_LINK.matcher(input);
				String link = null;
				while (m.find()) {
					link = m.group(1);
					String [] ns = link.split(":");
					if (ns.length > 1) {
						if (namespaces.contains(ns[0])) {
							try {
								links.write(link + "\n");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					else {
						try {
							links.write(link + "\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}

		}
		else {
			status = ERROR;
		}
		return status;
	}

	public void shutdown() {
		try {
			sites.flush();
			sites.close();
			links.flush();
			links.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @param args commandline arguments
	 */
	public static void main(String[] args) {
		Extractor extractor = new Extractor();
		BufferedReader reader = null;
		if (args.length == 0) {
			reader = new BufferedReader(new InputStreamReader(System.in));
		}
		else {
			try {
				reader  = new BufferedReader(new FileReader(args[0]));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			short status = Extractor.ERROR;
			while (reader.ready()) {
				String line = reader.readLine();
				short oldstatus = status;
				status = extractor.parseLine(line);
				if (oldstatus != status) {
		//			System.out.println("status: " + status + " line: " + line);
				}
			}
		} catch (IOException o) {
			System.out.println(o.getMessage());
		}
		extractor.shutdown();
	}

}
