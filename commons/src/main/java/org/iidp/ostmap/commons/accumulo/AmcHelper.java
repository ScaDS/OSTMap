/**
 * helper class to start and stop an accumulo mini cluster to be used in unit tests
 */
package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;

import java.io.File;
import java.io.IOException;

public class AmcHelper {

    private MiniAccumuloCluster amc;

    public MiniAccumuloCluster startMiniCluster(String tempStr){
        return startMiniCluster(new File(tempStr));
    }

    public MiniAccumuloCluster startMiniCluster(File tempFile) {

        try {
            amc = new MiniAccumuloCluster(tempFile, "password");
            amc.start();

            return amc;
        }catch (IOException | InterruptedException e){
            e.printStackTrace();
        }

        return null;
    }

    public Connector getConnector() {

        Instance instance = new ZooKeeperInstance(amc.getInstanceName(), amc.getZooKeepers());
        Connector conn = null;

        try {
            conn = instance.getConnector("root", new PasswordToken("password"));
        } catch (AccumuloException|AccumuloSecurityException e) {
            e.printStackTrace();
        }

        Authorizations auth = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());

        try {
            conn.securityOperations().changeUserAuthorizations("root", auth);
        } catch (AccumuloException|AccumuloSecurityException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void stopMiniCluster() {
        try{
            amc.stop();
        }catch (IOException| InterruptedException e){
            e.printStackTrace();
        }
    }
}
