package com.mgm.ring; /**
 * Created by mrblati on 25.04.16.
 */
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.accumulo.server.util.ListInstances;

public class MiniCluster {

    private static String zoohost = null;
    private static String zoohostFilePath =  (new File(System.getProperty("java.io.tmpdir"),"mac.tmp")).getAbsolutePath();


    public static void main(String[] args) throws IOException, InterruptedException {

        Logger.getRootLogger().setLevel(Level.WARN);

        System.out.println(zoohostFilePath);

        // run in Accumulo MAC
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");

        accumulo.start();


        System.out.println("starting up ...");
        Thread.sleep(3000);

        File instFile = new File(zoohostFilePath);
        instFile.deleteOnExit();
        System.out.println("cluster running with instance name " + accumulo.getInstanceName() + " and zookeepers " + accumulo.getZooKeepers());
        //String s[] = {"--print-all"};
        //ListInstances.main(s);
        //accumulo.

        try {
            FileWriter writer = new FileWriter(instFile);
            writer.write(accumulo.getZooKeepers());
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("hit Enter to shutdown ..");

        System.in.read();

        accumulo.stop();
    }

    public static String getZooHost() throws FileNotFoundException, IOException {
        if(zoohost == null) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(new File(zoohostFilePath)));
                zoohost = reader.readLine().trim();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        return zoohost;
    }
}