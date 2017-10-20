//import org.w3c.dom.Document
import org.w3c.dom.Node
import javax.xml.parsers.DocumentBuilderFactory
import scala.io.Source
import java.io.InputStream
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
//import org.cyberneko.html.parsers.DOMParser
//import org.cyberneko.html.parsers.SAXParser
import org.xml.sax.InputSource
import scala.collection.mutable.Queue
import scala.collection.mutable.HashSet
import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import scala.util.matching.Regex
import java.net.URLConnection
import java.net.URL
import org.jsoup.nodes.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.security.MessageDigest

/**
 * @author nox
 */

class Statistics {

  var nrOfStudent: Int = 0;
  var nrOfUniqueURLAnalyzed = 0;
  var nrOfUniqueURLSpotted = 0;
  var nrEnglishPages = 0;
  var nrGermanPages = 0;

}

class Crawler(val rootLink: String) {

  //private //val DomParser: DOMParser = new DOMParser()
  private val linkQueue = new Queue[String]
  private val spottedLinks = new HashSet[String]

  private var analyzedLinks = 0
  val stats = new Statistics
  var germanDict: Map[String, Double] = Map[String, Double]()
  var englishDict: Map[String, Double] = Map[String, Double]()
  type Shingle = List[String]
  var simHashSet: Set[List[Int]] = Set()
  var exactDuplicateCount: Int = 0;
  var nearDuplicateCount: Int = 0;

  def run() {

    //read dictionaries for language recoginition
    debugPrint("-------- Reading Dictionaries --------")
    readDictionaries()

    debugPrint("-------- Enqueue intial URL --------")
    debugPrint(rootLink)

    linkQueue.enqueue(rootLink)
    spottedLinks.add(rootLink)

    analyzedLinks = 0
    while (!linkQueue.isEmpty) {

      //println("QUEUE: "+linkQueue.size)
      //println("SPOTTED: "+spottedLinks.size)
      // println("ANALYZED: " + analyzedLinks)

      var url = linkQueue.dequeue();
      debugPrint("CRAWL: " + analyzedLinks + " analyzed. " + linkQueue.size + " in queue." + exactDuplicateCount + " duplicates, " + nearDuplicateCount + " near duplicates. " + "Students: "+stats.nrOfStudent +" English:"+ stats.nrEnglishPages+ " "+ "Next URL=" + url)

      var skip: Boolean = false
      val document = docDOMParse(url)
      document match {
        case Some(document) => handleDocument(document)
        case _              => println("Skipped")
      }

    }

    stats.nrOfUniqueURLAnalyzed = analyzedLinks
    stats.nrOfUniqueURLSpotted = spottedLinks.size

  }

  def docDOMParse(is: String): Option[Document] = {
    try {

      return Some(Jsoup.connect(is).timeout(500).get())
    } catch {

      case e: Exception => /*println(e.printStackTrace());*/ return None
    }

  }

  def handleDocument(document: Document) {

    /*----------Discover Links----------------
     * 
     * 
     * 
     * */
    //println("Document:" + document.getDocumentURI)
    analyzedLinks = analyzedLinks + 1
    val discoveredLinkTags: Elements = document.select("a[href]");
    var discoveredLinksNr: Int = discoveredLinkTags.size();

    var discoveredLinks: HashSet[String] = new HashSet[String]

    var i: Int = 0
    for (i <- 0 to discoveredLinksNr - 1) {

      var elem: Element = discoveredLinkTags.get(i);
      discoveredLinks.add(elem.attr("abs:href"))
      // println(elem.attr("abs:href"))
    }

    //println(discoveredLinks)

    val newDiscoveredLinksSrc = discoveredLinks.filter(x => isLegalLink(x) && !spottedLinks.contains(x))

    //  println(newDiscoveredLinksSrc)

    newDiscoveredLinksSrc.foreach { x => linkQueue.enqueue(x); spottedLinks.add(x) }

    /*----------Retrieve Content of Document----------------
         * 
         * 
         * 
         * */
   //document.getElementsByClass("print").remove()
    val words = document.select("div").text().split("\\s+").filter { x => !x.isEmpty() && x.head.isLetterOrDigit && !x.contains("https://") }.toList
   // println(words)
    // val words = removeUnnecessaryTags(document).getDocumentElement.getTextContent.split("\\s+").filter { x => !x.isEmpty() && x.head.isLetterOrDigit }.toList; //

    var duplicate = false

    if (words.size > 0) {
      val signature = getSimHash(words)

     
      val numIntersectingBits = simHashSet.map(x => x zip signature map { case (a, b) => a ^ b }).map { l => l.sum } /*.map { x => 1.0-(x.toDouble/128.0)}*/
      for (diffbits <- numIntersectingBits) {
        // println(diffbits)
        if (diffbits <= 0) {
          exactDuplicateCount = exactDuplicateCount + 1
          duplicate = true;

        } else if (diffbits <= 4) {
          nearDuplicateCount = nearDuplicateCount + 1
          duplicate = true
        }

      }

      simHashSet = simHashSet + signature
      //println(words)

      //var featureListChar = featureVector.map(x=>x.toList)

      //println(featureVector)

      /* Perform Statistics
         * 
         * 
         * 
         */

      //Count nr of students
      if (!duplicate) {
   

        //Language Recognition
        languageRecognition(words)

      }

      //students

      //language
      // similarity

    }
  }
  // Calculating SimHash

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }
  def getSimHash(words: List[String]): List[Int] = {

    // Map words => List[shingles-length3] => List[hashcode] => List[binary]
   var featureVector = createShinglesFromList(words, 3).map(_.hashCode()).map(h => binary32(h))

   //for 128 bits (slow)
  // var featureVector = createShinglesFromList(words, 3).map(x => md5(x.mkString)).map(h => binary128(h))
    
    
    // Map binaries from String to List[Char]
    // List[String] => List[List[Char]]
    var data1 = featureVector.map(x => x.toList)

    // Map binaries into List[Int]
    // List[List[Char]] => List[List[Int]]
    var data2 = data1.map(l => l.map(x => Integer.parseInt(x.toString, 10)))

    // Map each digit of binary (0,1) => (-1,1)
    // List[List[Int]] => List[List[Int]]
    var data3 = data2.map(l => l.map(x => (x * 2) - 1))

   
    // Column wise sum across binaries
    // Transpose
    var data4 = data3.transpose
    
  
    // Sum across rows (used to be columns)
    var data5 = data4.map(l => l.sum)

    // Map sums to document binary footprint
    //var data6 = data5.map(x=>(1+x.signum).signum)

    //*Seems to be ncessary*//
    var data6 = data5.map(x => (1 + x.signum).signum)

    //var data7 = data6.map(x=>x.toString)
    //var data8 = data7.mkString("")
    //var data9 = Integer.parseInt(data8,2)

    // println(data6)

    return data6

    //var signature = data9
  }

  def isLegalLink(link: String): Boolean = {
    /// only ending in html. No links that start with http:// https:// or www. as these lead outside of search domain

    return link.endsWith(".html") && link.startsWith("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/") /* /*!link.matches("http[\\s]?[\\:\\/\\/]*(?s).*")*/ && !link.matches("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/login(?-i)(?s).*") /*&& !link.matches("(?s).*(www(\\d)*\\.)+(?s).*")*/*/
  }

  
  //shingling
  def createShinglesFromList(words: List[String], shingleSize: Int): List[Shingle] = {
    require(shingleSize >= 1)
    words.sliding(shingleSize).toList
  }

  def createShinglesFromString(doc: String, shingleSize: Int): List[Shingle] =
    createShinglesFromList(doc.split("[ .,;:?!\t\n\r\f]+").toList, shingleSize)

  def binary128(ByteValues: Array[Byte]): String = {

    var i = 0
    var bits: String = ""
    val length = ByteValues.length;

    for (i <- 0 to length - 1) {

      bits += String.format("%8s", Integer.toBinaryString(ByteValues(i) & 0xFF)).replace(' ', '0');

    }

    // println(bits)
    return bits

  }

  def binary32(value: Int): String = {

    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0');

  }

  def readDictionaries() {

    var textIterD = io.Source.fromFile("DeDict").getLines()
    var textIterE = io.Source.fromFile("EnDict").getLines()

    for (lineD <- textIterD) {
      val keyValue = lineD.split("\t")

      germanDict.put(keyValue(0), keyValue(1).toDouble)
    }
    for (lineE <- textIterE) {
      val keyValue = lineE.split("\t")
      englishDict.put(keyValue(0), keyValue(1).toDouble)
    }

    //  println(germanDict)

  }

  def languageRecognition(words: List[String]) {
    val scoreGerman = words.map { word => math.log(germanDict.getOrElse(word.toLowerCase(), 1E-20)) }.sum
    val scoreEnglish = words.map { word => math.log(englishDict.getOrElse(word.toLowerCase(), 1E-20)) }.sum
    if (scoreEnglish > scoreGerman) {
      stats.nrEnglishPages = stats.nrEnglishPages + 1
           stats.nrOfStudent += words.view.count { x => x.matches("(?:^|\\W)(?i)student(?-i)(?:$|\\W)") }
    }

  }

  def debugPrint(toPrint: String) {
    println(toPrint)
  }

}


object Main {

  def main(args: Array[String]) {

    val c: Crawler = new Crawler("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html")

    c.run()

    //c.stats
    //output statistics

  }
}