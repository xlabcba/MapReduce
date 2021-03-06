package preProcess;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
// import java.util.logging.Level;
// import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import pageRank.Node;
import pageRank.PageRankDriver;

/**
 * @author lixie
 * Pre-process Mapper Class
 * to clean and parse raw page info into page name and its adjacency list,
 * and remove self links. Then for page itself, and each of its outlinks,
 * emit corresponding nodes.
 */
public class PreProcessMapper extends Mapper<LongWritable, Text, Text, Node> {

	private static Pattern namePattern;

	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
	}

	// Debugging Values
	// private Logger logger =
	// Logger.getLogger(PreProcessMapper.class.getName());

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Parse line to pageName:htmlContent and filter files
		String lineStr = value.toString();
		int delimLoc = lineStr.indexOf(':');
		String pageName = lineStr.substring(0, delimLoc);
		String html = lineStr.substring(delimLoc + 1);

		// Skip this html file with name containing "~"
		Matcher matcher = namePattern.matcher(pageName);
		if (!matcher.find()) {
			return;
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

			Node node = new Node();
			if (linkPageNames.size() != 0) {
				node.setAdjacencyList(linkPageNames);
			}
			// Emit (pageName, node)
			context.write(new Text(pageName), node);
			// Emit (outlinkName, node) in case some page not appear as source
			for (String outlinkName : linkPageNames) {
				context.write(new Text(outlinkName), new Node());
			}

		} catch (Exception e) {
			// Delete as invalid page
			context.getCounter(PageRankDriver.globalCounters.invalidPageCount).increment(1);
			// e.printStackTrace();
			// logger.log(Level.INFO, html);
			return;
		}
	}

}
