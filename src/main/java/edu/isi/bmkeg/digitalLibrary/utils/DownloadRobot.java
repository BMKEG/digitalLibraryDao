package edu.isi.bmkeg.digitalLibrary.utils;

import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.prefs.Preferences;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used to automate download processes by direct emulation of the keyboard and screen.
 * Since this program directly uses emulating mouse clicks on an external browser window, 
 * it should be consisdered a last ditch hack. 
 * 
 * TODO: NEEDS TESTING THIS IS NOT IMPLEMENTED IN THIS VERSION OF THE DIGITAL LIBRARY.
 * 
 * @author burns
 *
 */
public class DownloadRobot  {

  private static String stem =
      "C:/Documents and Settings/Administrator/My Documents/";
  private File inputFile;
  private File linesToOmit;
  private File outputDir;
  private String parseName;
  private int start = 0;
  private int end = 0;
  private Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
  private Robot robot;

  Map<String, Integer> keys;
  Set<String> shiftKeys;

  private LinkedHashSet toCheck = new LinkedHashSet();
  private String currentCheck;
  private LinkedHashSet checked = new LinkedHashSet();
  private ArrayList missed = new ArrayList();

  private List<String> citationsArray;
  private ArrayList views;
  private String model;

  private ArrayList executionQueue;

  protected int retStart;
  protected int retTotal;
  protected int recordStart = 1;
  protected int recordFinish = 20;
  protected int recordCount = 0;
  protected int recordSetSize = 20;

  public String getRunName() {
    return this.parseName;
  }

  private void initializeKeys () {
    this.keys = new HashMap<String, Integer>();

    keys.put( "A", new Integer(KeyEvent.VK_A) );
    keys.put( "B", new Integer(KeyEvent.VK_B) );
    keys.put( "C", new Integer(KeyEvent.VK_C) );
    keys.put( "D", new Integer(KeyEvent.VK_D) );
    keys.put( "E", new Integer(KeyEvent.VK_E) );
    keys.put( "F", new Integer(KeyEvent.VK_F) );
    keys.put( "G", new Integer(KeyEvent.VK_G) );
    keys.put( "H", new Integer(KeyEvent.VK_H) );
    keys.put( "I", new Integer(KeyEvent.VK_I) );
    keys.put( "J", new Integer(KeyEvent.VK_J) );
    keys.put( "K", new Integer(KeyEvent.VK_K) );
    keys.put( "L", new Integer(KeyEvent.VK_L) );
    keys.put( "M", new Integer(KeyEvent.VK_M) );
    keys.put( "N", new Integer(KeyEvent.VK_N) );
    keys.put( "O", new Integer(KeyEvent.VK_O) );
    keys.put( "P", new Integer(KeyEvent.VK_P) );
    keys.put( "Q", new Integer(KeyEvent.VK_Q) );
    keys.put( "R", new Integer(KeyEvent.VK_R) );
    keys.put( "S", new Integer(KeyEvent.VK_S) );
    keys.put( "T", new Integer(KeyEvent.VK_T) );
    keys.put( "U", new Integer(KeyEvent.VK_U) );
    keys.put( "V", new Integer(KeyEvent.VK_V) );
    keys.put( "W", new Integer(KeyEvent.VK_W) );
    keys.put( "X", new Integer(KeyEvent.VK_X) );
    keys.put( "Y", new Integer(KeyEvent.VK_Y) );
    keys.put( "Z", new Integer(KeyEvent.VK_Z) );

    keys.put( "a", new Integer(KeyEvent.VK_A) );
    keys.put( "b", new Integer(KeyEvent.VK_B) );
    keys.put( "c", new Integer(KeyEvent.VK_C) );
    keys.put( "d", new Integer(KeyEvent.VK_D) );
    keys.put( "e", new Integer(KeyEvent.VK_E) );
    keys.put( "f", new Integer(KeyEvent.VK_F) );
    keys.put( "g", new Integer(KeyEvent.VK_G) );
    keys.put( "h", new Integer(KeyEvent.VK_H) );
    keys.put( "i", new Integer(KeyEvent.VK_I) );
    keys.put( "j", new Integer(KeyEvent.VK_J) );
    keys.put( "k", new Integer(KeyEvent.VK_K) );
    keys.put( "l", new Integer(KeyEvent.VK_L) );
    keys.put( "m", new Integer(KeyEvent.VK_M) );
    keys.put( "n", new Integer(KeyEvent.VK_N) );
    keys.put( "o", new Integer(KeyEvent.VK_O) );
    keys.put( "p", new Integer(KeyEvent.VK_P) );
    keys.put( "q", new Integer(KeyEvent.VK_Q) );
    keys.put( "r", new Integer(KeyEvent.VK_R) );
    keys.put( "s", new Integer(KeyEvent.VK_S) );
    keys.put( "t", new Integer(KeyEvent.VK_T) );
    keys.put( "u", new Integer(KeyEvent.VK_U) );
    keys.put( "v", new Integer(KeyEvent.VK_V) );
    keys.put( "w", new Integer(KeyEvent.VK_W) );
    keys.put( "x", new Integer(KeyEvent.VK_X) );
    keys.put( "y", new Integer(KeyEvent.VK_Y) );
    keys.put( "z", new Integer(KeyEvent.VK_Z) );

    keys.put( "0", new Integer(KeyEvent.VK_0) );
    keys.put( "1", new Integer(KeyEvent.VK_1) );
    keys.put( "2", new Integer(KeyEvent.VK_2) );
    keys.put( "3", new Integer(KeyEvent.VK_3) );
    keys.put( "4", new Integer(KeyEvent.VK_4) );
    keys.put( "5", new Integer(KeyEvent.VK_5) );
    keys.put( "6", new Integer(KeyEvent.VK_6) );
    keys.put( "7", new Integer(KeyEvent.VK_7) );
    keys.put( "8", new Integer(KeyEvent.VK_8) );
    keys.put( "9", new Integer(KeyEvent.VK_9) );

    keys.put( "-", new Integer(KeyEvent.VK_MINUS) );
    keys.put( "=", new Integer(KeyEvent.VK_EQUALS) );
    keys.put( ".", new Integer(KeyEvent.VK_PERIOD) );
    keys.put( "/", new Integer(KeyEvent.VK_SLASH) );
    keys.put( "?", new Integer(KeyEvent.VK_SLASH) );
    keys.put( ":", new Integer(KeyEvent.VK_SEMICOLON) );
    keys.put( "'", new Integer(KeyEvent.VK_QUOTE) );

 /*
 * The maven builder wasn't to handle the following special characters [mt] 

    keys.put( "�", new Integer(KeyEvent.VK_A) );
    keys.put( "�", new Integer(KeyEvent.VK_O) );
    keys.put( "�", new Integer(KeyEvent.VK_O) );
    keys.put( "�", new Integer(KeyEvent.VK_U) );
    keys.put( "�", new Integer(KeyEvent.VK_A) );
    keys.put( "�", new Integer(KeyEvent.VK_A) );
    keys.put( "�", new Integer(KeyEvent.VK_A) );
    keys.put( "�", new Integer(KeyEvent.VK_C) );
    keys.put( "�", new Integer(KeyEvent.VK_E) );
    keys.put( "�", new Integer(KeyEvent.VK_E) );
    keys.put( "�", new Integer(KeyEvent.VK_E) );
    keys.put( "�", new Integer(KeyEvent.VK_I) );
    keys.put( "�", new Integer(KeyEvent.VK_I) );
    keys.put( "�", new Integer(KeyEvent.VK_N) );
    keys.put( "�", new Integer(KeyEvent.VK_O) );
    keys.put( "�", new Integer(KeyEvent.VK_O) );
    keys.put( "�", new Integer(KeyEvent.VK_O) );
    keys.put( "�", new Integer(KeyEvent.VK_U) );
    keys.put( "�", new Integer(KeyEvent.VK_Y) );
*/
    
    shiftKeys = new HashSet<String>();
    shiftKeys.add("?");
    shiftKeys.add(":");
    shiftKeys.add("A");
    shiftKeys.add("B");
    shiftKeys.add("C");
    shiftKeys.add("D");
    shiftKeys.add("E");
    shiftKeys.add("F");
    shiftKeys.add("G");
    shiftKeys.add("H");
    shiftKeys.add("I");
    shiftKeys.add("J");
    shiftKeys.add("K");
    shiftKeys.add("L");
    shiftKeys.add("M");
    shiftKeys.add("N");
    shiftKeys.add("O");
    shiftKeys.add("P");
    shiftKeys.add("Q");
    shiftKeys.add("R");
    shiftKeys.add("S");
    shiftKeys.add("T");
    shiftKeys.add("U");
    shiftKeys.add("V");
    shiftKeys.add("W");
    shiftKeys.add("X");
    shiftKeys.add("Y");
    shiftKeys.add("Z");
/*
 * The maven builder wasn't to handle the following special characters [mt] 
    shiftKeys.add("�");
    shiftKeys.add("�");
    shiftKeys.add("�");
    shiftKeys.add("�");
*/
  }

  private int[] getKeyEventCode(String s) throws Exception {
    int[] ii = new int[s.length()];

    for( int i=0; i<s.length(); i++) {
      String key = s.substring(i, i+1).toLowerCase(Locale.ENGLISH);
      Integer code = (Integer) this.keys.get(key);
      if( code == null ){
        throw new Exception("What is " + key + "?\n");
      }
      ii[ i ] = code.intValue();
    }

    return ii;

  }

  public DownloadRobot( File inputFile,
                        File toOmit,
                        File outputDir,
                        String parseName,
                        int start,
                        int end) {

    this.parseName = parseName;
    this.start = start;
    this.end = end;
    this.inputFile = inputFile;
    this.linesToOmit = toOmit;
    this.outputDir = outputDir;
    try {
      this.initializeKeys();
      this.robot = new Robot();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }



  public DownloadRobot( File inputFile,
                        File outputDir,
                        String parseName,
                        int start,
                        int end) {

    this.parseName = parseName;
    this.start = start;
    this.end = end;
    this.inputFile = inputFile;
    this.outputDir = outputDir;
    try {
      this.initializeKeys();
      this.robot = new Robot();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  public void runRobot() {

    try  {

      //
      //   1.
      //   Make sure that IE is running and that it's maximised in
      //   the foreground using the SAMIE Perl library
      //
      Preferences pref = Preferences.userNodeForPackage(DownloadRobot.class);
      String IELocation = pref.get("BrowserLocation", "");
      Process process = Runtime.getRuntime().exec(IELocation + " http://www.google.com/");

      List<String> lines = this.getLines();

      //Pattern p = Pattern.compile("^(\\w+)\\s+(I+\\s+)(.*)$");
      Iterator<String> it = lines.iterator();
      while (it.hasNext()) {
        String line = it.next();

        this.runRobotForLine(line);

      }

      process.destroy();

    }

    catch (Exception e) {
      e.printStackTrace();

    }

  }

  private void runRobotForLine( String line ) throws Exception {

    String[] l = line.split("\\t");
    String fileName = l[0] + "-" + l[1] + "-" + l[2] + "-" + l[3] + ".pdf";
    File f = new File( stem + fileName );
    File d2 = new File( stem + "pdfs/" + l[2] );
    File f2 = new File( stem + "pdfs/" + l[2] + "/" + fileName );

    if (f.exists()) {
      if (!d2.exists())
        d2.mkdir();
      if( !f2.exists() )
        this.moveFile(f, f2);
      if( f2.exists() )
        f.delete();
      return;
    }

    Thread.sleep(3000);

    robot.mouseMove(288, 103);
    robot.mousePress(InputEvent.BUTTON1_MASK);
    robot.delay(500);
    robot.mouseRelease(InputEvent.BUTTON1_MASK);
    String addr = l[4];

    StringSelection ss = new StringSelection(addr);
    clipboard.setContents(ss, ss);

    Thread.sleep(100);
    robot.keyPress(KeyEvent.VK_CONTROL);
    robot.keyPress(KeyEvent.VK_V);
    robot.keyRelease(KeyEvent.VK_V);
    robot.keyRelease(KeyEvent.VK_CONTROL);

    Thread.sleep(100);
    robot.keyPress(KeyEvent.VK_ENTER);
    robot.keyRelease(KeyEvent.VK_ENTER);

    //
    //   2.
    //   Give it 6 seconds, then reload
    //
    Thread.sleep(6000);
    robot.mouseMove(179, 69);
    robot.delay(5000);
    robot.mousePress(InputEvent.BUTTON1_MASK);
    robot.delay(500);
    robot.mouseRelease(InputEvent.BUTTON1_MASK);


    //
    //   3.
    //   Give it 12 seconds, then press the 'save button'
    //
    Thread.sleep(12000);
    robot.mouseMove(65, 154);
    robot.delay(500);
    robot.mousePress(InputEvent.BUTTON1_MASK);
    robot.delay(500);
    robot.mouseRelease(InputEvent.BUTTON1_MASK);

    //
    //   3.
    //   Enter the filename and hit go
    //
    ss = new StringSelection(fileName);
    clipboard.setContents(ss, ss);

    Thread.sleep(100);
    robot.keyPress(KeyEvent.VK_CONTROL);
    robot.keyPress(KeyEvent.VK_V);
    robot.keyRelease(KeyEvent.VK_V);
    robot.keyRelease(KeyEvent.VK_CONTROL);

    Thread.sleep(100);
    robot.keyPress(KeyEvent.VK_ENTER);
    robot.keyRelease(KeyEvent.VK_ENTER);

    //
    //   4.
    //   Move file to folder
    //
    Thread.sleep(1000);
    if( !d2.exists() ) {
      d2.mkdir();
    }
    this.moveFile(f, f2);

  }

  private void printHeader(URLConnection conn) {
    int n = 1; // n=0 has no key, and the HTTP return status in the value field
    boolean done = false;
    while (!done) {
      String headerKey = conn.getHeaderFieldKey(n);
      String headerVal = conn.getHeaderField(n);
      if (headerVal != null) {
        System.out.println(headerKey + "=" + headerVal);
      } else {
        done = true;
      }
      n++;
    }
  }

  private void moveFile( File from, File to) throws Exception {

    FileInputStream fis = new FileInputStream(from);
    FileOutputStream fos = new FileOutputStream(to);

    byte[] buf = new byte[1024];
    int size = 0;
    while ( (size = fis.read(buf)) != -1) {
      fos.write(buf, 0, size);
    }

    fis.close();
    fos.close();

    from.delete();

  }

  protected List<String> fileToLines(File citations) throws Exception {
    List<String> lines = new ArrayList<String>();

    if (end == 0) {
      end = 1000000;
    }

    Pattern p = Pattern.compile("^(\\w+)\\s+(I+\\s+)(.*)$");

    FileReader fr = new FileReader(citations);
    BufferedReader br = new BufferedReader(fr);
    String line = null;
    int c = 0;
    while ( (line = br.readLine()) != null) {
      Matcher m = p.matcher(line);
      if (m.find()) {
        line = m.group(1) + "\t" + m.group(3);
      }
      if (c >= start - 1 && c < end)
        lines.add(line);
      else if (c >= end)
        break;
      c++;
    }
    fr.close();

    return lines;

  }

  private void printLetters (File citations) throws Exception {
    Set<Character> chars = new LinkedHashSet<Character>();

    FileReader fr = new FileReader(citations);
    BufferedReader br = new BufferedReader(fr);
    String line = null;
    while ( (line = br.readLine()) != null) {
      for( int i=0; i<line.length(); i++) {
        char c = line.charAt(i);
        chars.add(new Character(c));
      }
    }
    fr.close();

    Object[] array = chars.toArray();
    Arrays.sort(array);
    chars = new LinkedHashSet<Character>();
    for( int i=0; i<array.length; i++) {
      chars.add( (Character) array[i]);
      System.out.println(array[i]);
    }

  }

  /**
   * Get either the citations file or the array as an ArrayList
   * @return ArrayList, citation format = 1stAuthor year vol page
   */
  private List<String> getLines() throws Exception {
    List<String> lines = null;
    if (this.citationsArray != null) {
      lines = this.citationsArray;
    } else if (this.inputFile != null) {
      lines = this.fileToLines(this.inputFile);
    } else {
      throw new Exception("No citation file or array is set");
    }

    if( this.linesToOmit != null ) {
      List<String> dropLines = this.fileToLines(this.linesToOmit);
      for( int i=0; i<dropLines.size(); i++ ) {
        String dropLine = (String) dropLines.get(i);
        String[] ll = dropLine.split("\\t");
        String dropCondition = ll[1] + "\t" + ll[2] + "\t" + ll[3];
        dropLines.set(i, dropCondition);
      }

      DROPLINES: for( int j=0; j<dropLines.size(); j++ ) {
        String dropLine = (String) dropLines.get(j);

        for( int i=0; i<lines.size(); i++ ) {
          String line = (String) lines.get(i);
          if( line.indexOf(dropLine) != -1 ){
            lines.remove(i);
            System.out.println( "DROPPING " + line );
            continue DROPLINES;
          }
        }

      }

    }

    return lines;
  }

  public static void main(String[] args) {

    Preferences prefs = Preferences.userNodeForPackage(DownloadRobot.class);
    File pdfDirAddr = new File( prefs.get("PDFDirectory", "") );

    DownloadRobot r = new DownloadRobot(new File("jcnPlusUrls.txt"),
                                        new File("toOmit.txt"),
                                        pdfDirAddr,
                                        "jcn-check",
                                        11,
                                        12000);

    try {
      r.runRobot();
//        r.printLetters(r.inputFile);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
