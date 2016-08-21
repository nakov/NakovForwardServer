/**
 * CheckAliveThread checks all dead servers in the server list and updates the
 * list when some dead server becomes alive. Checking is done on a beforehand
 * specified time itrervals. A server is considered alive if it accepts client
 * connections on the specified port.
 */
 
import java.io.IOException;
import java.net.Socket;
 
 
public class CheckAliveThread extends Thread
{
	private NakovForwardServer mNakovForwardServer = null;
 
    /**
     * Creates a check alive thread. NakovForwardServer object is needed
     * for obtaining the servers list.
     */
    public CheckAliveThread(NakovForwardServer aNakovForwardServer)
    {
        mNakovForwardServer = aNakovForwardServer;
    }
 
    /**
     * Until stopped checks all dead servers if they are alive and waits
     * specified time interval
     */
    public void run()
    {
        while (!interrupted()) {
            try {
               Thread.sleep(mNakovForwardServer.getCheckAliveIntervalMs());
           } catch (InterruptedException ie)     {
               ie.printStackTrace();
           }
           checkAllDeadServers();
        }
    }
 
    /**
     * Checks all dead servers if they are alive and updates their state if needed.
     */
    private void checkAllDeadServers()
    {
        NakovForwardServer.ServerDescription[] servers = mNakovForwardServer.getServersList();
        for (int i=0; i<servers.length; i++) {
           if (!servers[i].isAlive)
               if (alive(servers[i].host, servers[i].port))     {
                   servers[i].isAlive = true;
               }
        }
    }
 
    /**
     * Checks if given server is alive (if accepts client connections on specified port)
     */
    private boolean alive(String host, int port)
    {
        boolean result = false;
        try {
           Socket s = new Socket(host, port);
           result = true;
           s.close();
        } catch (IOException ioe) {
           ioe.printStackTrace();
           // Ignore unsuccessfull connect attempts
        }
        return result;
    }
 
}