package com.fnz.db2.journal.retrieve;

public class StringHelpers {
	 /**
	  * After the program run, invoke getReceiver() to get the instance of the Receiver, which will provide the returning result.
	  * @return
	  */

	 /**
	  * Pad space to the left of the input string s, so that the length of s is n.
	  * @param s
	  * @param n
	  * @return
	  */
	 public static String padLeft(String s, int n) {
	     return String.format("%1$" + n + "s", s);  
	 }
	  
	 /**
	  * Pad space to the right of the input string s, so that the length of s is n.
	  * @param s
	  * @param n
	  * @return
	  */
	 public static String padRight(String s, int n) {
	      return String.format("%1$-" + n + "s", s);  
	 }
	 
	 public static String safeTrim(String s) {
	     if (s == null)
	         return s;
	     return s.trim();
	 }
}
