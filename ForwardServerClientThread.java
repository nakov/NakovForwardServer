/**
 * ForwardServerClientThread handles the clients of Nakov Forward Server. It
 * finds suitable server from the server pool, connects to it and starts
 * the TCP forwarding between given client and its assigned server. After
 * the forwarding is failed and the two threads are stopped, closes the sockets.
 */
 
import java.net.Socket;
import java.net.SocketException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
 
public class ForwardServerClientThread extends Thread
{
    private NakovForwardServer mNakovForwardServer = null;
    private NakovForwardServer.ServerDescription mServer = null;
    private Socket mClientSocket = null;
    private Socket mServerSocket = null;
    private boolean mBothConnectionsAreAlive = false;
    private String mClientHostPort;
    private String mServerHostPort;
 
    /**
     * Creates a client thread for handling clients of NakovForwardServer.
     * A client socket should be connected and passed to this constructor.
     * A server socket is created later by run() method.
     */
    public ForwardServerClientThread(NakovForwardServer aNakovForwardServer, Socket aClientSocket)
    {
        mNakovForwardServer = aNakovForwardServer;
        mClientSocket = aClientSocket;
    }
 
    /**
     * Obtains a destination server socket to some of the servers in the list.
     * Starts two threads for forwarding : "client in <--> dest server out" and
     * "dest server in <--> client out", waits until one of these threads stop
     * due to read/write failure or connection closure. Closes opened connections.
     */
    public void run()
    {
        try {
           mClientHostPort = mClientSocket.getInetAddress().getHostAddress() + ":" + mClientSocket.getPort();
 
           // Create a new socket connection to one of the servers from the list
           mServerSocket = createServerSocket();
           if (mServerSocket == null) {  // If all the servers are down
               System.out.println("Can not establish connection for client " +
                   mClientHostPort + ". All the servers are down.");
               try { mClientSocket.close(); } catch (IOException e) {}
               return;
           }
 
           // Obtain input and output streams of server and client
           InputStream clientIn = mClientSocket.getInputStream();
           OutputStream clientOut = mClientSocket.getOutputStream();
           InputStream serverIn = mServerSocket.getInputStream();
           OutputStream serverOut = mServerSocket.getOutputStream();
 
           mServerHostPort = mServer.host + ":" + mServer.port;
           mNakovForwardServer.log("TCP Forwarding  " + mClientHostPort + " <--> " + mServerHostPort + "  started.");
 
           // Start forwarding of socket data between server and client
           ForwardThread clientForward = new ForwardThread(this, clientIn, serverOut);
           ForwardThread serverForward = new ForwardThread(this, serverIn, clientOut);
           mBothConnectionsAreAlive = true;
           clientForward.start();
           serverForward.start();
 
        } catch (IOException ioe) {
           ioe.printStackTrace();
        }
    }
 
    /**
     * connectionBroken() method is called by forwarding child threads to notify
     * this thread (their parent thread) that one of the connections (server or client)
     * is broken (a read/write failure occured). This method disconnects both server
     * and client sockets causing both threads to stop forwarding.
     */
    public synchronized void connectionBroken()
    {
        if (mBothConnectionsAreAlive) {
           // One of the connections is broken. Close the other connection and stop forwarding
           // Closing these socket connections will close their input/output streams
           // and that way will stop the threads that read from these streams
           try { mServerSocket.close(); } catch (IOException e) {}
           try { mClientSocket.close(); } catch (IOException e) {}
 
           mBothConnectionsAreAlive = false;
           mServer.clientsConectedCount--;
 
           mNakovForwardServer.log("TCP Forwarding  " + mClientHostPort + " <--> " + mServerHostPort + "  stopped.");
        }
    }
 
    /**
     * @return a new socket connected to some of the servers in the destination
     * servers list. Sequentially a connection to the least loaded server from
     * the list is tried to be established. If connecting to some alive server
     * fail, this server it marked as dead and next alive server is tried. If all
     * the servers are dead, null is returned. Thus if at least one server is alive,
     * a connection will be established (of course after some delay) and the system
     * will not fail (it is fault tolerant). Dead servers can be marked as alive if
     * revived, but this is done later by check alive thread.
     */
    private Socket createServerSocket() throws IOException
    {
        while (true) {
           mServer = getServerWithMinimalLoad();
           if (mServer == null)  // All the servers are down
               return null;
           try {
               Socket socket = new Socket(mServer.host, mServer.port);
               mServer.clientsConectedCount++;
               return socket;
           } catch (IOException ioe) {
               mServer.isAlive = false;
           }
        }
    }
 
    /**
     * @return the least loaded alive server from the server list if load balancing
     * is enabled or first alive server from the list if load balancing algorithm is
     * disabled or null if all the servers in the list are dead.
     */
    private NakovForwardServer.ServerDescription getServerWithMinimalLoad()
    {
        NakovForwardServer.ServerDescription minLoadServer = null;
        NakovForwardServer.ServerDescription[] servers = mNakovForwardServer.getServersList();
        for (int i=0; i<servers.length; i++) {
           if (servers[i].isAlive) {
               if ((minLoadServer==null) || (servers[i].clientsConectedCount < minLoadServer.clientsConectedCount))
                   minLoadServer = servers[i];
               // If load balancing is disabled, return first alive server
               if (!mNakovForwardServer.isLoadBalancingEnabled())
                   break;
            }
        }
        return minLoadServer;
    }
 
}
