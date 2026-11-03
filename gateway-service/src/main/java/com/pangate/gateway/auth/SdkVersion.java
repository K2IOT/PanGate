package com.pangate.gateway.auth;

/** Compares dotted numeric SDK versions ("1.10.0" > "1.9.9"); a suffix after '-' or '+' is ignored. */
public final class SdkVersion {

  private SdkVersion() {}

  public static int compare(String a, String b) {
    String[] x = core(a);
    String[] y = core(b);
    for (int i = 0; i < Math.max(x.length, y.length); i++) {
      int c = compareSegment(i < x.length ? x[i] : "0", i < y.length ? y[i] : "0");
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  private static String[] core(String v) {
    if (v == null) {
      return new String[0];
    }
    int cut = v.length();
    for (int i = 0; i < v.length(); i++) {
      char ch = v.charAt(i);
      if (ch == '-' || ch == '+') {
        cut = i;
        break;
      }
    }
    String c = v.substring(0, cut).trim();
    return c.isEmpty() ? new String[0] : c.split("\\.");
  }

  /** Numeric comparison on digit strings of any length; a non-numeric segment counts as 0. */
  private static int compareSegment(String a, String b) {
    String x = digits(a);
    String y = digits(b);
    return x.length() != y.length() ? Integer.compare(x.length(), y.length()) : x.compareTo(y);
  }

  private static String digits(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return "";
      }
    }
    int i = 0;
    while (i < s.length() && s.charAt(i) == '0') {
      i++;
    }
    return s.substring(i);
  }
}
