package com.threathunter.mock.util;

import org.apache.commons.beanutils.BeanToPropertyValueTransformer;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Random;


/**
 * 
 */
public class PropertyUtil {

  public static String getRandomStr(int length) {
    String KeyString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    StringBuffer sb = new StringBuffer();
    int len = KeyString.length();
    for (int i = 0; i < length; i++) {
      sb.append(KeyString.charAt((int) Math.round(Math.random() * (len - 1))));
    }
    return sb.toString();
  }

  public static String getRandomIP() {
    String[] array = {"183.250.108.118", "70.54.27.232", "106.85.33.97", "222.82.107.16",
        "171.8.112.230", "120.52.76.4", "117.136.32.30",
        "106.45.193.75", "110.184.218.105", "36.102.237.74", "112.96.100.163", "121.69.20.2",
        "122.227.97.226", "183.60.175.110", "124.228.212.248", "14.20.118.209", "42.92.105.144",
        "117.136.31.251", "36.248.206.233", "58.246.97.90", "36.248.206.22", "36.248.206.108",
        "36.248.206.51", "116.228.199.6",
        "36.248.206.41", "36.248.206.122", "36.248.207.226", "36.248.207.231", "36.248.206.115",
        "61.138.177.15", "61.158.148.51",
        "36.248.207.120", "36.248.207.101", "36.248.206.29", "61.158.148.88", "218.78.210.72",
        "61.158.146.248", "116.247.104.66",
        "60.221.122.51", "61.54.163.220", "61.54.164.59", "58.248.237.152", "117.63.116.72",
        "61.167.28.54", "58.22.214.113", "60.222.165.60",
        "58.246.245.102", "61.138.104.238", "103.7.28.45", "103.7.29.172", "120.27.116.113",
        "61.54.245.39", "61.138.121.28", "61.54.227.75",
        "121.28.205.70", "61.158.152.207", "58.243.250.25", "58.253.155.241", "60.220.232.2",
        "36.250.156.139", "61.136.81.162", "121.28.200.94",
        "60.222.237.139", "203.195.164.61", "58.253.159.161", "60.220.206.137", "61.186.158.21",
        "61.136.81.166"};
    Random r = new Random();
    int length = array.length;
    return array[r.nextInt(length - 1)];
  }

  public static String getRandomUser() {
    String[] array = {"Aaron", "Algernon", "Baron", "Bartley", "Berg", "Cornelius", "Dunn", "Frank",
        "Griffith", "Hobart", "Jerome", "Kennedy", "Lester", "Maximilian", "Murphy", "Nigel",
        "Osmond", "Patrick", "Rachel", "Silvester"};
    Random r = new Random();
    int length = array.length;
    return array[r.nextInt(length - 1)];
  }

  public static String getRandomDevice() {
    String[] array = {"device0000000001", "device0000000002", "device0000000003",
        "device0000000004", "device0000000005", "device0000000006", "device0000000007",
        "device0000000008", "device0000000009", "device0000000000"};
    Random r = new Random();
    int length = array.length;
    return array[r.nextInt(length - 1)];
  }

  public static String getRandomMobile() {
    String[] array = {"13422164342", "13420871736", "13239524934", "13544022591", "13418999851",
        "13169213547", "13421396487", "13414459076", "13416542007", "13510343120", "13424278031",
        "13421387045", "13662499632", "13417586734", "13249162381", "13420300394", "13432179494",
        "13216383463", "13410414079", "13427631548", "13411681685", "13417116749", "13028401067",
        "13266099791", "13410575545", "13427668941", "13410555215", "13046853762", "13516612954",
        "15914612945", "13434842894", "13265053440", "13418925352", "13410900174", "13237796476",
        "13168514837", "13267678246", "13415445543", "13450489853", "13418410584", "13418062926",
        "13418522893", "13299427256", "13267283764", "13035767814", "13531438005", "13510442574",
        "13365764470", "13028426847", "13185732405"};
    Random r = new Random();
    int length = array.length;
    return array[r.nextInt(length - 1)];
  }

  public static Collection collect(Collection collect, String propertyName) {
    return CollectionUtils.collect(collect, new BeanToPropertyValueTransformer("id"));
  }
}
