/**
 * Nakov TCP Socket Forward Server - freeware
 * Version 1.0 - March, 2002
 * (c) 2001 by Svetlin Nakov - http://www.nakov.com
 *
 * Short decription: Nakov Forward Server is designed to forward (redirect) TCP
 * connections from a client to a server choosen from a servers list. When a client
 * is connected to Nakov Forward Server, a new connection is opened to some of the
 * specified destination servers and all the traffic from destination server to
 * Nakov Forward Server is redirected to the client and also all the traffic from
 * the client to Nakov Forward Server is redirected to destination server. That
 * way Nakov Forward Server makes transparent redirection of TCP connections.
 * The data transfer schema is the following:
 *
 *     CLIENT <--> NAKOV_FORWARD_SERVER <--> DESTINATION_SERVER
 *
 * Clients and Destination Servers communicate only with Nakov Forward Server.
 *
 * Nakov Forward Server supports failt tolerance. When some of the servers in the
 * list fail to respond to TCP connect request (dead server), Nakov Forward Server
 * tries the next server in the list until it finds alive server. All dead servers
 * are checked if they are alive through some time interval and when some server
 * becomes available, it is added to alive list. When no server is alive, no
 * connection will be established.
 *
 * Nakov Forward Server supports also load balancing features. If load balancing
 * is enabled, when a client connection is accepted, Nakov Forward Server will
 * redirect the client to the least loaded server from the servers list. We consider
 * the server which hast minimal alive connections established by Nakov Forward
 * Server is least loaded.
 *
 * What we gain when we use Nakov Proxy Server?
 *     - Destination server does not know the real IP of the client. It thinks
 * that the IP of Nakov Forward Server is its client. Thus we can use a server
 * licensed for one IP address on several machines simultaneously.
 *     - Nakov Forward Server can run on a port number that is allowed by the
 * firewall and forward to a port number that is not allowed by firewall. Thus,
 * started on a server in a local network, it can give access to some disabled
 * by the firewall services.
 *     - Nakov Forward Server can give access to multiple clients to some service
 * that is allowed only for some fixed IP address when started on the machine
 * with this IP.
 *     - Fault Tolerance (failover) of Nakov Forward Server helps to avoid faults
 * when some of the servers die. Of course there is special hardware for this, but
 * it has high price. Instead you can use Nakov Forward Server (that is free).
 * If you setup several Nakov Forward Servers configured to use the same set of
 * destination servers and if you configure your routers to use redirect traffic
 * to both servers, you will obtain a stable fault tolerance system. In such a
 * system you have guarantee that crash of any of the servers (including some of
 * the Nakov Forward Servers) will not stop the service that these servers provide.
 * Of course the destination servers should run in a cluster and replicate their
 * sessions.
 *     - Load balancing helps to avoid overloading of the servers by distributing
 * the clients between them. Of course this should be done by special hardware
 * called "load balancer", but if we don't have such hardware, we can still use
 * this technology. When we use load balancing, all the servers in the list should
 * be running in a cluster and there should be no matter what of the servers the
 * client is connected to. The servers should communicate each other and replicate
 * their session data.
 *
 * NakovForwardServer.properties configuration file contains all the settings of
 * Nakov Forward Server. The only mandatory field is "Servers"
 * Destination servers should be in following format:
 *     Servers = server1:port1, server2:port2, server3:port3, ...
 * For example:
 *     Servers = 192.168.0.22:80, rakiya:80, 192.168.0.23:80, www.nakov.com:80
 * Nakov Forward Server listening port should be in format:
 *     ListeningPort = some_port (in range 1-65535)
 * Using load balancing algorithm is specified by following line:
 *     LoadBalancing = Yes/No
 * Check alive interval through which all dead threads should be re-checked if
 * they are alive is specified by following line:
 *     CheckAliveInterval = time_interval (in milliseconds)
 */
 
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.StringTokenizer;
 
public class NakovForwardServer
{
    private static final boolean ENABLE_LOGGING = true;
    public static final String SETTINGS_FILE_NAME = "NakovForwardServer.properties";
 
    private ServerDescription[] mServersList = null;
    private int mListeningTcpPort = 2001;
    private boolean mUseLoadBalancingAlgorithm = true;
    private long mCheckAliveIntervalMs = 5*1000;
 
    /**
     * ServerDescription descripts a server (server hostname/IP, server port,
     * is the server alive at last check, how many clients are connected to it, etc.)
     */
    class ServerDescription
    {
        public String host;
        public int port;
        public int clientsConectedCount = 0;
        public boolean isAlive = true;
        public ServerDescription(String host, int port)
        {
           this.host = host;
           this.port = port;
        }
    }
 
    /**
     * @return an array of ServerDescription - all destination servers.
     */
    public ServerDescription[] getServersList()
    {
        return mServersList;
    }
 
    /**
     * @return the time interval (in milliseconds) through which all dead servers
     * should be re-checked if they are alive (a server is alive if accepts
     * client connections on the specified port, otherwise is dead).
     */
    public long getCheckAliveIntervalMs()
    {
        return mCheckAliveIntervalMs;
    }
 
    /**
     * @return true if load balancing algorithm is enabled.
     */
    public boolean isLoadBalancingEnabled()
    {
        return mUseLoadBalancingAlgorithm;
    }
 
    /**
     * Reads the Nakov Forward Server configuration file "NakovForwardServer.properties"
     * and load user preferences. This method is called once during the server startup.
     */
    public void readSettings()
    throws Exception
    {
        // Read properties file in a Property object
        Properties props = new Properties();
        props.load(new FileInputStream(SETTINGS_FILE_NAME));
 
        // Read and parse the server list
        String serversProperty = props.getProperty("Servers");
        if (serversProperty == null )
           throw new Exception("The server list can not be empty.");
        try {
           ArrayList servers = new ArrayList();
           StringTokenizer stServers = new StringTokenizer(serversProperty,",");
           while (stServers.hasMoreTokens()) {
               String serverAndPort = stServers.nextToken().trim();
               StringTokenizer stServerPort = new StringTokenizer(serverAndPort,": ");
               String host = stServerPort.nextToken();
               int port = Integer.parseInt(stServerPort.nextToken());
               servers.add(new ServerDescription(host,port));
           }
           mServersList = (ServerDescription[]) servers.toArray(new ServerDescription[] {});
        } catch (Exception e) {
           throw new Exception("Invalid server list format : " + serversProperty);
        }
        if (mServersList.length == 0)
           throw new Exception("The server list can not be empty.");
 
        // Read server's listening port number
        try {
           mListeningTcpPort = Integer.parseInt(props.getProperty("ListeningPort"));
        } catch (Exception e) {
           log("Server listening port not specified. Using default port : " + mListeningTcpPort);
        }
 
        // Read load balancing property
        try {
           String loadBalancing = props.getProperty("LoadBalancing").toLowerCase();
            mUseLoadBalancingAlgorithm = (loadBalancing.equals("yes") ||
				loadBalancing.equals("true") || loadBalancing.equals("1") ||
				loadBalancing.equals("enable") || loadBalancing.equals("enabled"));
        } catch (Exception e) {
           log("LoadBalancing property is not specified. Using default value : " + mUseLoadBalancingAlgorithm);
        }
 
        // Read the check alive interval
        try {
           mCheckAliveIntervalMs = Integer.parseInt(props.getProperty("CheckAliveInterval"));
        } catch (Exception e) {
           log("Check alive interval is not specified. Using default value : " + mCheckAliveIntervalMs + " ms.");
        }
 
    }
 
    /**
     * Starts a thread that re-checks all dead threads if they are alive
     * through mCheckAliveIntervalMs millisoconds
     */
    private void startCheckAliveThread()
    {
        CheckAliveThread checkAliveThread = new CheckAliveThread(this);
        checkAliveThread.setDaemon(true);
        checkAliveThread.start();
    }
 
    /**
     * Starts the forward server - binds on a given port and starts serving
     */
    public void startForwardServer()
    throws Exception
    {
        // Bind server on given TCP port
        ServerSocket serverSocket;
        try {
           serverSocket = new ServerSocket(mListeningTcpPort);
        } catch (IOException ioe) {
           throw new IOException("Unable to bind to port " + mListeningTcpPort);
        }
 
        log("Nakov Forward Server started on TCP port " + mListeningTcpPort + ".");
        log("All TCP connections to " + InetAddress.getLocalHost().getHostAddress() + 
			":" + mListeningTcpPort + " will be forwarded to the following servers:");
        for (int i=0; i<mServersList.length; i++) {
           log("  " + mServersList[i].host +  ":" + mServersList[i].port);
        }
        log("Load balancing algorithm is " + (mUseLoadBalancingAlgorithm ? "ENABLED." : "DISABLED."));
 
        // Accept client connections and process them until stopped
        while(true) {
           try {
               Socket clientSocket = serverSocket.accept();
               String clientHostPort = clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
               log("Accepted client from " + clientHostPort);
               ForwardServerClientThread forwardThread = new ForwardServerClientThread(this, clientSocket);
               forwardThread.start();
           } catch (Exception e) {
               throw new Exception("Unexpected error.\n" + e.toString());
           }
        }
    }
 
    /**
     * Prints given log message on the standart output if logging is enabled,
     * otherwise ignores it
     */
    public void log(String aMessage)
    {
        if (ENABLE_LOGGING)
           System.out.println(aMessage);
    }
 
    /**
     * Program entry point. Reads settings, starts check-alive thread and
     * the forward server
     */
    public static void main(String[] aArgs)
    {
        NakovForwardServer srv = new NakovForwardServer();
        try {
           srv.readSettings();
           srv.startCheckAliveThread();
           srv.startForwardServer();
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
 
}
