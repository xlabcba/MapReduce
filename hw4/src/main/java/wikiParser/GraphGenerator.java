package wikiParser;

import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class GraphGenerator {

	private static Pattern namePattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
	}
    
	public static String createGraph(String line) {
		
		// Parse line to pageName:htmlContent and filter files
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);

		// Skip this html file with name containing "~"
		Matcher matcher = namePattern.matcher(pageName);
		if (!matcher.find()) {
			return null;
		}

		try {
			// Configure Parser
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names
			List<String> linkPageNames = new LinkedList<String>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

			// Parse html to list of linkPageNames
			html = html.replace("&", "&amp;"); // Replace &, or else SAXParser exception
			xmlReader.parse(new InputSource(new StringReader(html)));

			// Remove self link, which is not allowed
			for (Iterator<String> li = linkPageNames.iterator(); li.hasNext();) {
				String currName = li.next();
				if (currName.equals(pageName)) {
					li.remove();
				}
			}

			// Return as String with format PageName~Outlink1~Outlink2...
			String adjacencyList = String.join("~", linkPageNames);
			return pageName + "~" + adjacencyList;

		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}
	}
}
