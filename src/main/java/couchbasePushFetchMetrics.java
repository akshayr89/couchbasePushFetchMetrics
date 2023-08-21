import com.google.gson.JsonObject;
import com.knowesis.persist.PandaCache;
import com.knowesis.persist.PandaCacheClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class couchbasePushFetchMetrics {
    public PandaCache pandaCache = null;

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            //System.out.println("Usage couchbasePushFetchMetrics.jar <strPersistAddressList> <persistUser> <persistPassword> <sslCertificatePath> <bucketName> <operation (push/pull)> <docListFile>");
            System.out.println("Usage couchbasePushFetchMetrics.jar <sslCertificatePath> <operation(set/get)> <docListFile>");
            System.exit(0);
        }

        String strPersistAddressList = "10.193.33.106,10.193.33.238,10.193.34.15,10.193.35.50";//args[ 0 ];
        String persistUser = "opolonp";//args[ 1 ];
        String persistPassword = "0polodef23";//args[ 2 ];
        String sslCertificatePath = args[ 0 ];
        String bucketName = "opolo_default_np";//args[4];
        String operation = args[1];
        String docListFile = args[2];



        System.out.println("Starting couchbasePushFetchMetrics utility...");


        // Get the Document list from file in an string array.
        ArrayList<String> docArray = getDocListFromFile(docListFile);

        switch (operation) {
            case "set":
                setToBucket(docArray,strPersistAddressList , persistUser, persistPassword, bucketName, sslCertificatePath);
                break;
            case "get":
                getFromBucket(docArray,strPersistAddressList , persistUser, persistPassword, bucketName, sslCertificatePath);
                break;
            default:
                throw new Exception("Invalid Operation to Utility. Valid operations are: 'push' or  'get' ");


        }


}
    public static ArrayList<String> getDocListFromFile(String filename) {
        // Creating an empty ArrayList of string type
        ArrayList<String> al = new ArrayList<String>();
        try {
            //the file to be opened for reading
            FileInputStream fileInputStream =new FileInputStream(filename);
            Scanner sc=new Scanner(fileInputStream);    //file to be scanned

            System.out.println("Reading file: "+ filename);

            while(sc.hasNextLine())
            {
                //Creating Producer record
                String docName = sc.nextLine();
                //Add to arrayList
                al.add(docName);
            }
            sc.close();     //closes the scanner
            System.out.println("Number of documents in File "+ filename + " : " + al.size());
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return al;

    }
    private static void getFromBucket(ArrayList<String> docArray, String strPersistAddressList, String persistUser, String persistPassword, String bucketName, String sslCertificatePath) throws Exception {
        couchbasePushFetchMetrics persist = new couchbasePushFetchMetrics();
        persist.pandaCache = new PandaCacheClient().initialisePandaCache( "couchbasev7", strPersistAddressList , persistUser, persistPassword, bucketName, sslCertificatePath );

        System.out.println( "Get Connection Established !!!" );

        // Get
        long startTime = System.nanoTime();
        for (int i = 0; i < docArray.size(); i++) {
            //Set document in bucket
            persist.pandaCache.get( docArray.get(i) );
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        double elapsedTimeInSecond = (double) elapsedTime / 1000000000;

        System.out.println("Get Operation Completed in: "+ elapsedTime + " nanoSecs | Seconds: "+ elapsedTimeInSecond);


    }

    private static void setToBucket(ArrayList<String> docArray, String strPersistAddressList, String persistUser, String persistPassword, String bucketName, String sslCertificatePath) throws Exception {

        //Create json object to Push
        JsonObject jo = new JsonObject();
        jo.addProperty( "status", "Active" );
        jo.addProperty( "type", "plainDocument" );

        // Make connection
        couchbasePushFetchMetrics persist = new couchbasePushFetchMetrics();
        persist.pandaCache = new PandaCacheClient().initialisePandaCache( "couchbasev7", strPersistAddressList , persistUser, persistPassword, bucketName, sslCertificatePath );
        System.out.println( "Set Connection Established !!!" );

        // Push
        long startTime = System.nanoTime();
        for (int i = 0; i < docArray.size(); i++) {
            //Set document in bucket
            persist.pandaCache.set(docArray.get(i),jo);
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        double elapsedTimeInSecond = (double) elapsedTime / 1000000000;

        System.out.println("Set Operation Completed in: "+ elapsedTime + " nanoSecs | Seconds: "+ elapsedTimeInSecond);

    }
}
