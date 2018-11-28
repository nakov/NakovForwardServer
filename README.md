# NakovForwardServer (TCP Reverse Proxy)
<b>TCP port forwarding software</b>, written in Java: forwards local TCP port (e.g. 127.0.0.1:1521) to external network port (e.g. 0.0.0.0:1522). Works as <b>reverse proxy</b> server (like nginx) to tunnel a TCP connection through external port to an internal server+port in a local network.

<b>Nakov TCP Socket Forward Server</b> - freeware
Version 1.0 - March, 2002
(c) 2001 by Svetlin Nakov - http://www.nakov.com

<b>Short decription</b>: Nakov Forward Server is designed to forward (redirect) TCP connections from a client to a server choosen from a servers list. When a client is connected to Nakov Forward Server, a new connection is opened to some of the specified destination servers and all the traffic from destination server to Nakov Forward Server is redirected to the client and also all the traffic from
the client to Nakov Forward Server is redirected to destination server. That way Nakov Forward Server makes transparent redirection of TCP connections.

The TCP connection data transfer goes like this:

    CLIENT  <-->  NAKOV_FORWARD_SERVER  <-->  DESTINATION_SERVER
    
Clients and destination servers communicate only with Nakov Forward Server.

Typical usage scenario is to access an internal network resource from Internet by TCP port forwarding:

        Internet IP         <-->    NakovForwardServer       <-->     Internal network IP
    (e.g. 62.176.119.226)         (e.g. 212.50.7.153:8888)          (e.g. 192.168.1.117:80)

Nakov Forward Server supports failt tolerance. When some of the servers in the list fail to respond to TCP connect request (dead server), Nakov Forward Server tries the next server in the list until it finds alive server. All dead servers are checked if they are alive through some time interval and when some server becomes available, it is added to alive list. When no server is alive, no connection will be established.

Nakov Forward Server supports also load balancing features. If load balancing is enabled, when a client connection is accepted, Nakov Forward Server will redirect the client to the least loaded server from the servers list. We consider the server which hast minimal alive connections established by Nakov Forward Server is least loaded.

# Benefits of Using Nakov Forward Server?

 - Destination server does not know the real IP of the client. It thinks that the IP of Nakov Forward Server is its client. Thus we can use a server licensed for one IP address on several machines simultaneously.

 - Nakov Forward Server can run on a port number that is allowed by the firewall and forward to a port number that is not allowed by firewall. Thus, started on a server in a local network, it can give access to some disabled by the firewall services.

 - Nakov Forward Server can give access to multiple clients to some service that is allowed only for some fixed IP address when started on the machine with this IP.

 - Fault Tolerance (failover) of Nakov Forward Server helps to avoid faults when some of the servers die. Of course there is special hardware for this, but it has high price. Instead you can use Nakov Forward Server (that is free). If you setup several Nakov Forward Servers configured to use the same set of destination servers and if you configure your routers to use redirect traffic to both servers, you will obtain a stable fault tolerance system. In such a system you have guarantee that crash of any of the servers (including some of the Nakov Forward Servers) will not stop the service that these servers provide. Of course the destination servers should run in a cluster and replicate their sessions.

 - Load balancing helps to avoid overloading of the servers by distributing the clients between them. Of course this should be done by special hardware called "load balancer", but if we don't have such hardware, we can still use this technology. When we use load balancing, all the servers in the list should be running in a cluster and there should be no matter what of the servers the client is connected to. The servers should communicate each other and replicate their session data.

# Configuration

<code>NakovForwardServer.properties</code> configuration file contains all the settings of Nakov Forward Server. The only mandatory field is "Servers". Destination servers should be described in following format:

    Servers = server1:port1, server2:port2, server3:port3, ...
    
For example:

    Servers = 192.168.0.22:80, rakiya:80, 192.168.0.23:80, www.nakov.com:80
    
Nakov Forward Server listening port should be in format:

    ListeningPort = some_port (in range 1-65535)
    
Check alive interval through which all dead threads should be re-checked if they are alive is specified by following line:

    CheckAliveInterval = time_interval (in milliseconds)

Using load balancing algorithm is specified by following line:

    LoadBalancing = Yes/No
    
# Compilation

    javac *.java
    
# Running the server

    java NakovForwardServer

# ngrok
Finally, if you have more advanced needs, you might find this localhost tunelling software useful: https://ngrok.com
