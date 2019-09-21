package ca.uwaterloo.cs451.a0;

import java.io.File;
import java.util.Scanner;
import java.lang.Math;


public class findTotal
{
	public String giveCount(String word) throws Exception
	  {
		  
			  File file = new File("temp/part-r-00000");
			  Scanner sc = new Scanner(file);
		  while (sc.hasNextLine())
                {
                        String temp = sc.nextLine();
                  
                        String yo = "";
                String[] arrOfStr = temp.split("\t");

                if(arrOfStr[0].equals(word))
			return(arrOfStr[1]);
		  }
		   return("No");
	   }



 public static void main(String[] args) throws Exception {
  System.out.println(Integer.ParseInt(giveCount("a")));
}
}
