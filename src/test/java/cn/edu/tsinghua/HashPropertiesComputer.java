package cn.edu.tsinghua;

import java.util.Random;

public class HashPropertiesComputer {


  public static void main(String[] argv) {
    int[] ra = new int[50];

    int times = 100000;
    for (int i = 0; i < times; i++) {
      ra[Math.abs(generateString(10).hashCode()) % 50]++;
    }

    for (int i = 0; i < 50; i++) {
      System.out.println(String.format("%s:\t%s", i, getPrettyStr(ra[i])));
    }

  }

  private static final String allChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private static String generateString(int length) {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      sb.append(allChar.charAt(random.nextInt(allChar.length())));
    }
    return sb.toString();
  }

  private static String getPrettyStr(int num) {
    StringBuilder stringBuilder = new StringBuilder();
    int temp = num / 100;
    for (int i = 0; i < temp; i++) {
      stringBuilder.append("■");
    }
    temp = (num % 100) / 10;
    for (int i = 0; i < temp; i++) {
      stringBuilder.append("□");
    }
    temp = num % 100;
    for (int i = 0; i < temp; i++) {
      stringBuilder.append("*");
    }
    return stringBuilder.toString();
  }

}
