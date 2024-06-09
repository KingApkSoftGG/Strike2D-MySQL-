package strikeprofile;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
//import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.dbcp2.BasicDataSource;

import java.util.Base64;


@SuppressWarnings("unchecked")
public class Main {
	final static int version=12;//versija servera
	final static int ipport=37490;//port
	final static int sspport=37489;//strikeserver connect port
	final static int plength=1440;//buffer size
	final static int thrcount=4;//kol-vo worker potokov
	final static int minclientver=11;//min. client version
	final static int sysuserid=2;//id sistemnogo usera
	final static String strDateFormat = "dd.MM.yy HH:mm";
	static Thread selectorThread;
	static Selector selector;
	static ServerSocketChannel serverSocket;
	static SelectionKey serverKey;
	static ByteBuffer rbuf;
	static int selectorthrid=0;
	static DatabaseConnection dbpool=null;
	static Thread[] tworker = new Thread[thrcount];
	static Worker[] worker = new Worker[thrcount];
	static int[] workerid = new int[thrcount];
	static HashMap<SocketChannel, LinkedList<ByteBuffer>>[] pendingData;

	static {
		pendingData = new HashMap[thrcount];
	}

	static LinkedList<SocketChannel>[] socketlist = new LinkedList[thrcount];
	final static HashMap<SocketChannel, Integer> socket_thread = new HashMap<>();
	static Random rnd = new Random();
	static boolean log=false;//log on/off
	static boolean getKey=false;//getAppCode
	static long lastprovselkeys=0;//wremja poslednei proverki selector keys
	
	//udp socket
	static DatagramSocket mainUdpSocket;
    static Thread mainReciver;
    static byte[] mainRbuf = new byte[plength];
    static DatagramPacket mainRpacket;

	public static void main(String[] args) {
		log("Start profile-server time: "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		log("Version: "+version);
		log("----------------------");
		log("Kommand list:");
		log("1. exit");
		log("2. cl - Clients count");
		log("3. log - Log On/Off");
		log("4. getkey - getAppCode mode On/Off");
		log("----------------------");
		boolean error=false;
		try {
			dbpool = new DatabaseConnection();
		}catch(Exception e){
			error=true;
			log("Start DB connection pool - error: "+ e);
		}
		if (!error){
			log("Start DB connection pool - OK");
			try{
				Connection conn=dbpool.getConnection();
				conn.close();
				log("Test SQL connection - OK");
			}catch (SQLException e){
				log("Test SQL connection - error: "+ e);
			}
		}
		for (int i = 0; i < thrcount; i++) {
			pendingData[i] = new HashMap<>();
			socketlist[i] = new LinkedList<>();
			workerid[i]++;
			worker[i] = new Worker(i);
			tworker[i] = new Thread(worker[i]);
			tworker[i].setName("worker id="+ i);
			tworker[i].setDaemon(true);
			tworker[i].start();
		}
		try {
			mainUdpSocket= new DatagramSocket(sspport);
			mainUdpSocket.setReceiveBufferSize(plength*20);
			mainRpacket = new DatagramPacket(mainRbuf, mainRbuf.length);
			log("Start main UDP Socket - OK");
		} catch (IOException e) {
			log("Start main UDP Socket - error: "+ e);
			return;
		}
		mainReciver = new Thread(() -> {
            mainReciver.setPriority(Thread.MAX_PRIORITY);
            log("Start main UDP receiver thread - OK");
            while (mainUdpSocket!=null && !mainUdpSocket.isClosed()) {
                try {
                    mainUdpSocket.receive(mainRpacket);
                    int praz=mainRpacket.getLength();
                    InetAddress ip=mainRpacket.getAddress();
                    int port=mainRpacket.getPort();
                    if (praz<1) continue;
                    byte[] data = mainRpacket.getData();
                    ByteArrayInputStream bais=new ByteArrayInputStream(data, 0, praz);
                    DataInputStream dais=new DataInputStream(bais);
                    byte pname=dais.readByte();
                    if (pname==10){//_checkencrcode_
                        byte sid=dais.readByte();
                        byte gver=dais.readByte();
                        byte cver=dais.readByte();
                        int rid=dais.readInt();
                        String tmpid=dais.readUTF();
                        String encrCode=dais.readUTF();
                        String cip=dais.readUTF();
                        dais.close();
                        bais.close();
                        int result=checkEncrCode(encrCode, cip, gver, cver, tmpid);
                        ByteArrayOutputStream baos=new ByteArrayOutputStream();
                        DataOutputStream daos=new DataOutputStream(baos);
                        daos.writeByte(10);
                        daos.writeInt(rid);
                        daos.writeInt(sid);
                        daos.writeUTF(cip);
                        daos.writeByte(result);
                        daos.close();
                        final byte[] bytes=baos.toByteArray();
                        DatagramPacket pak = new DatagramPacket(bytes, bytes.length, ip, port);
                        mainUdpSocket.send(pak);
                        continue;
                    }
                    if (pname==11){//_checkuser_
                        int srvid=dais.readInt();
                        byte status=dais.readByte();
                        String tmpid=dais.readUTF();
                        int rid=dais.readInt();
                        byte sid=dais.readByte();
                        dais.close();
                        bais.close();
                        int result=checkUser(srvid, status, tmpid);
                        ByteArrayOutputStream baos=new ByteArrayOutputStream();
                        DataOutputStream daos=new DataOutputStream(baos);
                        daos.writeByte(11);
                        daos.writeInt(srvid);
                        daos.writeInt(rid);
                        daos.writeByte(sid);
                        daos.writeByte(result);
                        daos.close();
                        final byte[] bytes=baos.toByteArray();
                        DatagramPacket pak = new DatagramPacket(bytes, bytes.length, ip, port);
                        mainUdpSocket.send(pak);
                        continue;
                    }
                    if (pname==12){//_checkapp_
                        int appCode=dais.readInt();
                        int sid=dais.readInt();
                        String cip=dais.readUTF();
                        dais.close();
                        bais.close();
                        int result=checkApp(appCode, cip);
                        ByteArrayOutputStream baos=new ByteArrayOutputStream();
                        DataOutputStream daos=new DataOutputStream(baos);
                        daos.writeByte(12);
                        daos.writeInt(appCode);
                        daos.writeInt(sid);
                        daos.writeUTF(cip);
                        daos.writeByte(result);
                        daos.close();
                        final byte[] bytes=baos.toByteArray();
                        DatagramPacket pak = new DatagramPacket(bytes, bytes.length, ip, port);
                        mainUdpSocket.send(pak);
                        continue;
                    }
                    dais.close();
                    bais.close();
                } catch (IOException e) {
                    log("Main UDP Socket: "+ e);
                } catch (NullPointerException e) {
                    log("Main UDP Socket error: "+ e);
                }
            }
        });
		mainReciver.setDaemon(true);
		mainReciver.setName("mainReciver");
		mainReciver.start();
		selectorthrid++;
		selectorThread = new Thread(() -> {
            int id=selectorthrid;
            System.out.println("Start selector thread - OK");
            try {
                selector = Selector.open();
                serverSocket = ServerSocketChannel.open();
                serverSocket.bind(new InetSocketAddress(ipport));
                serverSocket.configureBlocking(false);
                serverKey=serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            } catch (IOException e) {
                log("Server socket init error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
                return;
            }
            rbuf = ByteBuffer.allocate(plength);
            while (id==selectorthrid) {
                long currtime=System.currentTimeMillis();
                if (currtime-lastprovselkeys>10000){
                    lastprovselkeys=currtime;
                    provselectorkeys();
                }
                for (int i = 0; i < thrcount; i++) {
                    synchronized(socketlist[i]) {
                        if (!socketlist[i].isEmpty()){
                            for (SocketChannel socket : socketlist[i]) {
                                try {
                                    socket.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
                                } catch (CancelledKeyException | NullPointerException e) {
                                }
                            }
                            socketlist[i].clear();
                        }
                    }
                }
                try {
                    selector.select();
                } catch (IOException e) {
                    log("selector error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
                } catch (ClosedSelectorException e){
                    log("selector error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
                    break;
                }
                if (selector==null || !selector.isOpen()){
                    log("Selector closed.");
                    break;
                }
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    try {
                        if (!key.isValid()) {
                            closechannel(key, false);
                            continue;
                        }
                        if (key.isAcceptable()) {
                            accept();
                        } else if (key.isReadable()) {
                            read(key);
                        } else if (key.isWritable()) {
                            write(key);
                        }
                    } catch (NullPointerException e){
                    }
                }
            }
        });
		selectorThread.setName("selector");
		selectorThread.setPriority(Thread.MAX_PRIORITY);
		selectorThread.setDaemon(true);
		selectorThread.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (br!=null){
			try {
				String s=br.readLine();
				if (s==null) continue;
				if (s.equals("exit")) {
					closeall();
					System.exit(0);
				}
				if (s.equals("cl")) {
					if (selector==null) continue;
					int count=selector.keys().size()-1;
					log("Current clients count="+count);
					continue;
				}
				if (s.equals("log")) {
					log=!log;
					if (log){
						log("Log - On");
					}else{
						log("Log - Off");
					}
					continue;
				}
				if (s.equals("getkey")) {
					getKey=!getKey;
					if (getKey){
						log("getAppCode - On");
					}else{
						log("getAppCode - Off");
					}
					continue;
				}
				log("Kommand \""+s+"\" not found.");
			} catch (IOException e) {
				log("Input error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}
		}
	}
	
	static void provselectorkeys(){//zakritie ne activnih kanalov
		try {
			if (selector==null) return;
			long curtime=System.currentTimeMillis();
			Set<SelectionKey> allkeys=selector.keys();
			for (SelectionKey key : allkeys){
				if (key.equals(serverKey)) continue;
				if (!key.isValid()) continue;
				ChannelTime ct = (ChannelTime)key.attachment();
				if (ct==null) continue;
				if (curtime-ct.time>10000) closechannel(key, false);
			}
		} catch (Exception e){
			log("provselectorkeys error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}
	}
	
	static void sqlclose(ResultSet rs, PreparedStatement ps, Connection conn){
		try {
			if (rs!=null) rs.close();
		} catch (SQLException e) {
		}
		try {
			if (ps!=null) ps.close();
		} catch (SQLException e) {
		}
		try {
			if (conn!=null) conn.close();
		} catch (SQLException e) {
		}
	}

	static void closechannel(SelectionKey key, boolean cancel){
		try {
			if (log) log("CLOSE");
			SocketChannel socketChannel = (SocketChannel) key.channel();
			if (cancel) key.cancel();
			for (int i = 0; i < thrcount; i++) {
				synchronized (pendingData[i]) {
					pendingData[i].remove(socketChannel);
				}
			}
			synchronized (socket_thread){
				socket_thread.remove(socketChannel);
			}
			socketChannel.close();
		} catch (IOException | NullPointerException | ConcurrentModificationException e) {
			log("closechannel error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}
	}

	static void accept() {
		try {
			SocketChannel socketChannel = serverSocket.accept();
			if (socketChannel==null) return;
			socketChannel.configureBlocking(false);
			socketChannel.register(selector, SelectionKey.OP_READ, new ChannelTime());
		} catch (IOException | CancelledKeyException e) {
			log("accept error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}
	}

	static void read(SelectionKey key) {
		ChannelTime ct = (ChannelTime)key.attachment();
		if (ct!=null) ct.time=System.currentTimeMillis();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		byte[] res=null;
		int rescount=0;
		if (log) log("READ");
		try {
			do {
				rbuf.clear();
				int count = socketChannel.read(rbuf);
				if (log) log("read count="+ count +" buf.remaining="+ rbuf.remaining());
				if (count == -1) {
					closechannel(key, true);
					return;
				}
				if (count==0 || !rbuf.hasArray()) break;
				if (res==null){
					res=rbuf.array().clone();
				}else{
					byte[] temp=res;
					res = new byte[temp.length+count];
					System.arraycopy(temp, 0, res, 0, temp.length);
					System.arraycopy(rbuf.array(), 0, res, temp.length, count);
				}
				rescount=rescount+count;
			} while (rbuf.remaining()<=0);		
		} catch (IOException | NotYetConnectedException e) {
			closechannel(key, true);
			return;
		} catch (UnsupportedOperationException | IndexOutOfBoundsException | ArrayStoreException |
				 NullPointerException e){
			log("read error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			return;
		}
		if (res==null || rescount<=0) return;
		int wid=-1;
		synchronized (socket_thread){
			if (socket_thread.containsKey(socketChannel)) wid = socket_thread.get(socketChannel);
		}
		if (wid==-1){
			wid=rnd.nextInt(thrcount);
			synchronized (socket_thread){
				socket_thread.put(socketChannel, wid);
			}
		}
		worker[wid].processData(socketChannel, res, rescount);
	}
	
	static void setkeyread(SelectionKey key){
		try{
			if (!key.isValid()) return;
			key.interestOps(SelectionKey.OP_READ);
		} catch (CancelledKeyException e){
			log("setkeyread error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}
	}

	static void write(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		if (socketChannel==null) return;
		int wid=-1;
		synchronized (socket_thread){
			if (socket_thread.containsKey(socketChannel)) {
                wid=socket_thread.get(socketChannel);
            }
		}
		if (wid==-1){
			setkeyread(key);
			return;
		}
		synchronized (pendingData[wid]) {
			List<ByteBuffer> queue = pendingData[wid].get(socketChannel);
			if (queue==null) {
				setkeyread(key);
				return;
			}
			while (!queue.isEmpty()) {
				ByteBuffer buf = queue.get(0);
				if (buf==null){
					queue.remove(0);
					continue;
				}
				try{
					if (log) log("write buf size="+buf.remaining());
					int count=socketChannel.write(buf);
					if (log) log("write count="+count);
				} catch (IOException e){
					if (log) log("write error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
					closechannel(key, true);
					return;
				} catch (NotYetConnectedException e){
					closechannel(key, true);
					return;
				}
				if (buf.remaining() > 0) break;
				queue.remove(0);
			}
			if (queue.isEmpty()) {
				pendingData[wid].remove(socketChannel);
				setkeyread(key);
			}
		}
	}
	
	static class ChannelTime{
		long time;
		ChannelTime(){
			this.time=System.currentTimeMillis();
		}
	}

	static class ServerDataEvent{
		SocketChannel socket;
		byte[] data;
		boolean complet;
		ServerDataEvent(SocketChannel socket, byte[] data, boolean complet){
			this.socket=socket;
			this.data=data;
			this.complet=complet;
		}
	}

	static class DatabaseConnection {
		BasicDataSource bds;

		DatabaseConnection() {
			bds = new BasicDataSource();
			bds.setDriverClassName("com.mysql.cj.jdbc.Driver");
			bds.setUrl("jdbc:mysql://185.82.219.196:3306/strike2dd?autoReconnect=true&useSSL=true&useUnicode=true&character_set_server=utf8mb4&character_set_client=utf8mb4");
			//[client] default-character-set = utf8mb4
			//[mysql] default-character-set = utf8mb4
			//[mysqld] character-set-client-handshake = FALSE character-set-server = utf8mb4 collation-server = utf8mb4_unicode_ci
			//Execute in mysql: SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
			bds.setUsername("pppserver");
			bds.setPassword("FjQ=6%205&807");
		}

		synchronized public Connection getConnection() throws SQLException{
            return bds.getConnection();
		}

		public void closeConnection(){
			try{
				if (bds!=null) bds.close();
			}catch(SQLException e){
				log("closeConnection error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}
		}
	}

	static class Worker implements Runnable {
		private final List<ServerDataEvent> queue = new LinkedList<>();
		final int gid;

		Worker(int id){
			this.gid=id;
		}

		public void processData(SocketChannel socket, byte[] data, int count) {
			if (data==null || count<=0) return;
			byte[] dataCopy = new byte[count];
			try {
				System.arraycopy(data, 0, dataCopy, 0, count);
			} catch (IndexOutOfBoundsException | ArrayStoreException | NullPointerException e){
				log("processData error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
				return;
			}
			boolean complet=(dataCopy[count-1]==0);
			synchronized(queue) {
				try{
					int addind=-1;
					for (int i = 0; i < queue.size(); i++) {
						ServerDataEvent sde=queue.get(i);
						if (sde.socket==socket && !sde.complet){
							addind=i;
							break;
						}
					}
					if (addind==-1){
						queue.add(new ServerDataEvent(socket, dataCopy, complet));
						if (log) log("add data count="+ count +" complet="+ complet);
					}else{
						ServerDataEvent sde=queue.get(addind);
						byte[] temp=sde.data;
						sde.data=new byte[temp.length+count];
						System.arraycopy(temp, 0, sde.data, 0, temp.length);
						System.arraycopy(dataCopy, 0, sde.data, temp.length, count);
						sde.complet=complet;
						if (log) log("comlet data addind="+ addind +" old count="+ temp.length +" new count="+ sde.data.length +" complet="+ complet);
					}
				} catch (IllegalArgumentException e){
					log("processData error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
					return;
				}
				queue.notify();
			}
		}

		public void run() {
			log("Start worker thread id="+ gid +" - OK");
			int wid=workerid[gid];
			while(wid==workerid[gid]) {
				ServerDataEvent dataEvent=null;
				boolean obr=false;
				synchronized(queue) {
					if(queue.isEmpty()) {
						try {
							queue.wait();
						} catch (InterruptedException e) {
						}
					}
					if(!queue.isEmpty()) {
						for (int i = 0; i < queue.size(); i++) {
							dataEvent = queue.get(i);
							if (!dataEvent.socket.isOpen()){
								queue.remove(i);
								continue;
							}
							if (dataEvent.data == null || !dataEvent.complet) {
								continue;
							}
							queue.remove(i);
							obr=true;
						}
					}
				}
				if (obr){
					int off=0;
                    assert dataEvent.data != null;
                    int len=dataEvent.data.length;
					String msg;
					for (int i=0; i<len; i++) {
						if (dataEvent.data[i]==0){
							msg=new String(dataEvent.data, off, i-off, StandardCharsets.UTF_8);
							obrpaket(dataEvent.socket, msg);
							off=i+1;
						}
					}
				}else{
					synchronized(queue){ 
						try {
							queue.wait();
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}
		
		List<String> getList(String msg){
			List<String> str = new ArrayList<>();
			int len=msg.length();
			if (len<=0) return str;
			int off=0;
			int pos;
			do {
				pos=msg.indexOf('\b', off);
				if (pos==-1){
					str.add(msg.substring(off));
				}else{
					str.add(msg.substring(off, pos));
					off=pos+1;
				}
			} while (pos!=-1);
			return str;
		}

		void obrpaket(SocketChannel socket, String msg){
			if (socket==null || msg==null) return;
			List<String> str = null;
			try{
				str = getList(msg);
				if (!str.isEmpty()){
					do {
						if (str.get(0).equals("_msglist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2);
							int did=Integer.parseInt(str.get(3));
							int lastmsgid=Integer.parseInt(str.get(4));
							frmsglist(socket, did, lastmsgid, tmpid, ver);
							break;
						}
						if (str.get(0).equals("_grmsglist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							int grpid;
							int lastmsgid;
							grpid=Integer.parseInt(str.get(2));
							lastmsgid=Integer.parseInt(str.get(3));
							grmsglist(socket, grpid, lastmsgid);
							break;
						}
						if (str.get(0).equals("_refr_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2);
							refr(tmpid, ver);
							break;
						}
						if (str.get(0).equals("_getsrv_")){
							getservers(socket);
							break;
						}
						if (str.get(0).equals("_stat_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<10) break;
							if (str.size()!=10) break;
							String tmpid=str.get(2).trim();
							int score=Integer.parseInt(str.get(3));
							int enemk=Integer.parseInt(str.get(4));
							int bugk=Integer.parseInt(str.get(5));
							int zomk=Integer.parseInt(str.get(6));
							int jugk=Integer.parseInt(str.get(7));
							int ghostk=Integer.parseInt(str.get(8));
							int death=Integer.parseInt(str.get(9));
							stat(tmpid, score, enemk, bugk, zomk, jugk, ghostk, death);
							break;
						}
						if (str.get(0).equals("_conntmp_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							String nik, lng;
							long appCode;
							if (ver>=12) {
								int gver=Integer.parseInt(str.get(2));
								int cver=Integer.parseInt(str.get(3));
								tmpid=str.get(4).trim();
								String key=getKey(gver, cver, tmpid);
								if (key==null){
									if (getKey) send(socket, gid, "_getkey_");
									break;
								}
								String enc=decryptStr(str.get(5).trim(), key);
								List<String> subStr = getList(enc);
								if (subStr.size()!=3) break;
								nik=subStr.get(0).trim();
								lng=subStr.get(1).trim();
								appCode=Long.parseLong(subStr.get(2));
							}else{
								nik=str.get(3).trim();
								lng=str.get(4).trim();
								appCode=Long.parseLong(str.get(5));
							}
							conntmp(socket, ver, tmpid, nik, lng, appCode);
							break;
						}
						if (str.get(0).equals("_grmsg_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							String frmsg=str.get(4).trim();
							grsendmsg(socket, tmpid, grpid, frmsg);
							break;
						}
						if (str.get(0).equals("_msg_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int did=Integer.parseInt(str.get(3));
							String frmsg=str.get(4).trim();
							frsendmsg(socket, tmpid, did, frmsg);
							break;
						}
						if (str.get(0).equals("_connlogin_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid="", login, pass, lng;
							long appCode;
							if (ver>=12) {
								int gver;
								gver = Integer.parseInt(str.get(2));
								int cver=Integer.parseInt(str.get(3));
								tmpid=str.get(4).trim();
								String key=getKey(gver, cver, tmpid);
								if (key==null){
									if (getKey) send(socket, gid, "_getkey_");
									break;
								}
								String encr=str.get(5).trim();
								String decr=decryptStr(encr, key);
								List<String> subStr = getList(decr);
								if (subStr.size()!=4) {
									if (getKey) send(socket, gid, "_getkey_");
									break;
								}
								login=subStr.get(0).trim();
								pass=subStr.get(1).trim();
								lng=subStr.get(2).trim();
								appCode=Long.parseLong(subStr.get(3));
							}else{
								login=str.get(2).trim();
								pass=str.get(3).trim();
								lng=str.get(4).trim();
								appCode=Long.parseLong(str.get(5));
							}
							connlogin(socket, ver, tmpid, login, pass, lng, appCode);
							break;
						}
						if (str.get(0).equals("_ltoday_")){
							ltoday(socket);
							break;
						}
						if (str.get(0).equals("_lweek_")){
							lweek(socket);
							break;
						}
						if (str.get(0).equals("_lall_")){
							lall(socket);
							break;
						}
						if (str.get(0).equals("_newnik_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							String nik=str.get(3).trim();
							newnik(tmpid, nik);
							break;
						}
						if (str.get(0).equals("_setnogr_")){
							String tmpid=str.get(2).trim();
							boolean nogr=Boolean.parseBoolean(str.get(3));
							setnogr(tmpid, nogr);
							break;
						}
						if (str.get(0).equals("_find_")){
							int srvid=Integer.parseInt(str.get(2));
							String fnik=str.get(3).trim();
							String fid=str.get(4).trim();
							find(socket, srvid, fnik, fid);
							break;
						}
						if (str.get(0).equals("_info_")){
							int ver=Integer.parseInt(str.get(1));
							int did=Integer.parseInt(str.get(2));
							info(socket, ver, did);
							break;
						}
						if (str.get(0).equals("_mapinfo_")){
							String mapuid=str.get(2);
							int usrvid=Integer.parseInt(str.get(3));
							mapinfo(socket, mapuid, 0, usrvid);
							break;
						}
						if (str.get(0).equals("_maplike_")){
							int mapid=Integer.parseInt(str.get(2));
							String utmpid=str.get(3);
							int urid=Integer.parseInt(str.get(4));
							int like=Integer.parseInt(str.get(5));
							maplike(socket, mapid, utmpid, urid, like);
							break;
						}
						if (str.get(0).equals("_frreq_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int did=Integer.parseInt(str.get(3));
							frreq(socket, tmpid, did);
							break;
						}
						if (str.get(0).equals("_addfr_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int did=Integer.parseInt(str.get(3));
							addfr(socket, tmpid, did);
							break;
						}
						if (str.get(0).equals("_addpart_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							int uid=Integer.parseInt(str.get(4));
							addpart(socket, tmpid, grpid, uid);
							break;
						}
						if (str.get(0).equals("_delpart_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							int uid=Integer.parseInt(str.get(4));
							delpart(socket, tmpid, grpid, uid);
							break;
						}
						if (str.get(0).equals("_statpart_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							int uid=Integer.parseInt(str.get(4));
							int newstat=Integer.parseInt(str.get(5));
							statpart(socket, tmpid, grpid, uid, newstat);
							break;
						}
						if (str.get(0).equals("_frndlist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2);
							frndlist(socket, tmpid, ver);
							break;
						}
						if (str.get(0).equals("_grplist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2);
							grplist(socket, tmpid, ver);
							break;
						}
						if (str.get(0).equals("_partlist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							int grpid=Integer.parseInt(str.get(2));
							partlist(socket, grpid);
							break;
						}
						if (str.get(0).equals("_frreqlist_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2);
							frreqlist(socket, tmpid, ver);
							break;
						}
						if (str.get(0).equals("_mapdauer_")){
							String mapuid=str.get(2).trim();
							int dauer=Integer.parseInt(str.get(3));
							int usrvid=Integer.parseInt(str.get(4));
							mapdauer(mapuid, dauer, usrvid);
							break;
						}
						if (str.get(0).equals("_getmymaps_")){
							String utmpid=str.get(2).trim();
							getmymaps(socket, utmpid);
							break;
						}
						if (str.get(0).equals("_getmap_")){
							String mapuid=str.get(2).trim();
							getmap(socket, mapuid);
							break;
						}
						if (str.get(0).equals("_getmaptop_")){
							int ver=Integer.parseInt(str.get(1));
							int topid=Integer.parseInt(str.get(2));
							getmaptop(socket, topid, ver);
							break;
						}
						if (str.get(0).equals("_findmap_")){
							int ver=Integer.parseInt(str.get(1));
							String mapname=str.get(2).trim();
							int mapid=0, mapuserid=0;
							try {
								mapid=Integer.parseInt(str.get(3));
							}catch (NumberFormatException e){
							}
							try {
								mapuserid=Integer.parseInt(str.get(4));
							}catch (NumberFormatException e){
							}
							findmap(socket, mapname, mapid, mapuserid, ver);
							break;
						}
						if (str.get(0).equals("_delmap_")){
							String utmpid=str.get(2).trim();
							String mapuid=str.get(3).trim();
							delmap(socket, utmpid, mapuid);
							break;
						}
						if (str.get(0).equals("_getnews_")){
							String lng=str.get(2).trim();
							getnews(socket, lng);
							break;
						}
						if (str.get(0).equals("_newgrp_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							String name=str.get(3).trim();
							newgrp(socket, tmpid, name);
							break;
						}
						if (str.get(0).equals("_delfrreq_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int did=Integer.parseInt(str.get(3));
							delfrreq(socket, tmpid, did);
							break;
						}
						if (str.get(0).equals("_delfrnd_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int did=Integer.parseInt(str.get(3));
							delfrnd(socket, tmpid, did);
							break;
						}
						if (str.get(0).equals("_leavegrp_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							leavegrp(socket, tmpid, grpid);
							break;
						}
						if (str.get(0).equals("_savemap_")){
							String tmpid=str.get(2).trim();
							int mapusrrvid=Integer.parseInt(str.get(3));
							String mapuid=str.get(4).trim();
							String timestamp=str.get(5).trim();
							String mapname=str.get(6).trim();
							int gamerid=Integer.parseInt(str.get(7));
							int mapw=Integer.parseInt(str.get(8));
							int maph=Integer.parseInt(str.get(9));
							int maplength=Integer.parseInt(str.get(10));
							int oblength=Integer.parseInt(str.get(11));
							String mapdata=str.get(12);
							String obdata=str.get(13);
							savemap(socket, tmpid, mapusrrvid, mapuid, timestamp, mapname, gamerid, mapw, maph, maplength, oblength, mapdata, obdata);
							break;
						}
						if (str.get(0).equals("_delgrp_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							int grpid=Integer.parseInt(str.get(3));
							delgrp(socket, tmpid, grpid);
							break;
						}
						if (str.get(0).equals("_register_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							String nik=str.get(3).trim();
							String login=str.get(4).trim();
							String pass=str.get(5);
							register(socket, tmpid, login, pass, nik);
							break;
						}
						if (str.get(0).equals("_delacc_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String login=str.get(2).trim();
							String pass=str.get(3).trim();
							delacc(socket, login, pass);
							break;
						}
						if (str.get(0).equals("_deltmpacc_")){
							String tmpid=str.get(2).trim();
							deltmpacc(socket, tmpid);
							break;
						}
						if (str.get(0).equals("_chpass_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String login=str.get(2).trim();
							String oldpass=str.get(3).trim();
							String newpass=str.get(4).trim();
							chpass(socket, login, oldpass, newpass);
							break;
						}
						if (str.get(0).equals("_getkey_")){
							int ver=Integer.parseInt(str.get(1));
							if (ver<minclientver) break;
							String tmpid=str.get(2).trim();
							String encrCode=str.get(3).trim();
							String key=getKey(33, 1, tmpid);
							String appCode=decryptStr(encrCode, key);
							log("appCode="+appCode);
						}
						if (str.get(0).equals("_addnews_")){
							String title=str.get(1).trim();
							String lng=str.get(2).trim();
							String news=str.get(3).trim();
							addnews(title, lng, news);
							break;
						}
					} while(false);
				}
			} catch (Exception e){
				log("obrpaket error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
				if (str!=null) for (String s : str) log(s);
			}
		}
		
		void addpart(SocketChannel socket, String tmpid, int grpid, int uid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0, stat=0;
			String unik, utime;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) srvid=rs.getInt(1);
				if (srvid==0 || srvid==uid){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_stat FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, srvid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()) stat=rs.getInt(1);
				if (stat!=1 && stat!=2){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT users_nik, users_wason, users_nogr FROM users WHERE users_id=?");
				ps.setInt(1, uid);
				rs=ps.executeQuery();
				if (rs.next()) {
					unik=rs.getString(1);
					utime=converttime(rs.getTimestamp(2), true);
					int nogr=rs.getByte(3);
					if (nogr==1){
						send(socket, gid, "_nogr_");
						sqlclose(rs, ps, conn);
						return;
					}
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_id FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, uid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()){
					send(socket, gid, "_addpartok_\b"+ grpid +"\b"+ uid +"\b"+unik+"\b"+utime);
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("INSERT INTO grppart (grppart_gid, grppart_uid) VALUES (?, ?)");
				ps.setInt(1, grpid);
				ps.setInt(2, uid);
				int row=ps.executeUpdate();
				if (row>0){
					send(socket, gid, "_addpartok_\b"+ grpid +"\b"+ uid +"\b"+unik+"\b"+utime);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("addpart error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void delpart(SocketChannel socket, String tmpid, int grpid, int uid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0, stat=0;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) srvid=rs.getInt(1);
				if (srvid==0 || srvid==uid){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_stat FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, srvid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()) stat=rs.getInt(1);
				if (stat!=1 && stat!=2){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_stat FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, uid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()) {
					int dstat=rs.getInt(1);
					if (dstat==1 || (dstat==2 && stat!=1)){
						send(socket, gid, "_delparterror_");
						sqlclose(rs, ps, conn);
						return;
					}
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("DELETE FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, uid);
				ps.setInt(2, grpid);
				int row=ps.executeUpdate();
				if (row>0){
					send(socket, gid, "_delpartok_\b"+ grpid +"\b"+ uid);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("delpart error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void statpart(SocketChannel socket, String tmpid, int grpid, int uid, int newstat){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0, stat=0;
			try{
				if (tmpid==null || tmpid.length()!=12 || (newstat!=0 && newstat!=2)) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) srvid=rs.getInt(1);
				if (srvid==0 || srvid==uid){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_stat FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, srvid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()) stat=rs.getInt(1);
				if (stat!=1){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("UPDATE grppart SET grppart_stat=? WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, newstat);
				ps.setInt(2, uid);
				ps.setInt(3, grpid);
				int row=ps.executeUpdate();
				if (row>0){
					send(socket, gid, "_statpartok_\b"+ grpid +"\b"+ uid +"\b"+ newstat);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("statpart error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void addfr(SocketChannel socket, String tmpid, int did){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid, frcount=0, maxfr;
			boolean add=true;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_maxfr FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
					maxfr=rs.getInt("users_maxfr");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT frreq_id FROM frreq WHERE (frreq_sid=? AND frreq_did=?)OR(frreq_sid=? AND frreq_did=?)");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				ps.setInt(3, did);
				ps.setInt(4, sid);
				rs=ps.executeQuery();
				if (!rs.next()){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT count(*) FROM frnd WHERE frnd_sid=?");
				ps.setInt(1, sid);
				rs=ps.executeQuery();
				if (rs.next()){
					frcount=rs.getInt(1);
				}
				if (frcount>=maxfr){
					send(socket, gid, "_addfrmax_\b"+ did);
					add=false;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT frnd_id FROM frnd WHERE frnd_sid=? AND frnd_did=?");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				rs=ps.executeQuery();
				if (rs.next()){
					send(socket, gid, "_addfrtwo_\b"+ did);
					add=false;
				}
				rs.close();
				ps.close();
				if (add){
					conn.setAutoCommit(false); 
					ps=conn.prepareStatement("INSERT INTO frnd (frnd_sid, frnd_did) VALUES (?, ?)");
					ps.setInt(1, sid);
					ps.setInt(2, did);
					ps.addBatch();
					ps.setInt(1, did);
					ps.setInt(2, sid);
					ps.addBatch();
					int[] res=ps.executeBatch();
					conn.commit();
					conn.setAutoCommit(true); 
					if (res.length==2 && res[0]>0 && res[1]>0){
						send(socket, gid, "_addfrok_\b"+ did);
					}else{
						send(socket, gid, "_error_");
						sqlclose(rs, ps, conn);
						return;
					}
					ps.close();
				}
				ps=conn.prepareStatement("DELETE FROM frreq WHERE (frreq_sid=? AND frreq_did=?)OR(frreq_sid=? AND frreq_did=?)");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				ps.setInt(3, did);
				ps.setInt(4, sid);
				ps.executeUpdate();
			}catch (Exception e){
				log("addfr error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void delfrreq(SocketChannel socket, String tmpid, int did){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("DELETE FROM frreq WHERE (frreq_sid=? AND frreq_did=?)OR(frreq_sid=? AND frreq_did=?)");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				ps.setInt(3, did);
				ps.setInt(4, sid);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_delfrreqok_\b"+ did);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("delfrreq error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void delgrp(SocketChannel socket, String tmpid, int grpid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0, stat=0;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) srvid=rs.getInt(1);
				if (srvid==0){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT grppart_stat FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, srvid);
				ps.setInt(2, grpid);
				rs=ps.executeQuery();
				if (rs.next()) stat=rs.getInt(1);
				if (stat!=1){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("DELETE FROM grp WHERE grp_id=?");
				ps.setInt(1, grpid);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_delgrpok_\b"+ grpid);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("delgrp error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void leavegrp(SocketChannel socket, String tmpid, int grpid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) srvid=rs.getInt(1);
				if (srvid==0){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("DELETE FROM grppart WHERE grppart_uid=? AND grppart_gid=?");
				ps.setInt(1, srvid);
				ps.setInt(2, grpid);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_leavegrpok_\b"+ grpid);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("leavegrp error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void delfrnd(SocketChannel socket, String tmpid, int did){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("DELETE FROM frnd WHERE (frnd_sid=? AND frnd_did=?)OR(frnd_sid=? AND frnd_did=?)");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				ps.setInt(3, did);
				ps.setInt(4, sid);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_delfrndok_\b"+ did);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("delfrnd error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void info(SocketChannel socket, int ver, int did){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_score, users_enemk, users_bugk, users_zomk, users_jugk, users_ghostk, users_death, users_wason, users_created FROM users WHERE users_id=?");
				ps.setInt(1, did);
				rs=ps.executeQuery();
				if (rs.next()){
					int score=rs.getInt("users_score");
					int enemk=rs.getInt("users_enemk");
					int bugk=rs.getInt("users_bugk");
					int zomk=rs.getInt("users_zomk");
					int jugk=rs.getInt("users_jugk");
					int ghostk=rs.getInt("users_ghostk");
					int death=rs.getInt("users_death");
					String restime=converttime(rs.getTimestamp("users_wason"), true);
					String registered=converttime(rs.getTimestamp("users_created"), false);
					rs.close();
					ps.close();
					if (log) log("info did="+ did +" thread id="+ gid);
					send(socket, gid, "_infook_\b"+ score +'\b'+ enemk +'\b'+ bugk +'\b'+ zomk +'\b'+ jugk +'\b'+ ghostk +'\b'+ death +'\b'+restime+'\b'+registered);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("info error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void mapinfo(SocketChannel socket, String mapuid, int mapid, int usrvid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				if (mapid==0) {
					ps=conn.prepareStatement("SELECT am_id, am_userid, users_nik, am_created, am_updated, am_mapw, am_maph, am_lcount, am_dcount, (SELECT ml_like FROM maplike WHERE ml_amid=am_id AND ml_userid=? LIMIT 1) AS mlike FROM allmaps LEFT JOIN users ON users_id=am_userid WHERE am_mapuid=?");
					ps.setInt(1, usrvid);
					ps.setString(2, mapuid);
				}else{
					ps=conn.prepareStatement("SELECT am_id, am_userid, users_nik, am_created, am_updated, am_mapw, am_maph, am_lcount, am_dcount, (SELECT ml_like FROM maplike WHERE ml_amid=am_id AND ml_userid=? LIMIT 1) AS mlike FROM allmaps LEFT JOIN users ON users_id=am_userid WHERE am_id=?");
					ps.setInt(1, usrvid);
					ps.setInt(2, mapid);
				}
				rs=ps.executeQuery();
				if (rs.next()){
					mapid=rs.getInt(1);
					int userid=rs.getInt(2);
					String nik=rs.getString(3);
					String created=converttime(rs.getTimestamp(4), false);
					String updated=converttime(rs.getTimestamp(5), false);
					int mapw=rs.getInt(6);
					int maph=rs.getInt(7);
					int lcount=rs.getInt(8);
					int dcount=rs.getInt(9);
					byte like=rs.getByte(10);
					rs.close();
					ps.close();
					send(socket, gid, "_mapinfook_\b"+ mapid +'\b'+ userid +'\b'+nik+'\b'+created+'\b'+updated+'\b'+ mapw +'\b'+ maph +'\b'+ lcount +'\b'+ dcount +'\b'+String.valueOf(like));
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("mapinfo error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void maplike(SocketChannel socket, int mapid, String utmpid, int urid, int like){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int userid=0, lcount, dcount;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, utmpid);
				rs=ps.executeQuery();
				if (rs.next()) userid=rs.getInt(1);
				if (userid==0) {
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				String ip="";
				try{
					InetSocketAddress addr=(InetSocketAddress)socket.getRemoteAddress();
					ip=String.valueOf(addr.getAddress());
				}catch(Exception ex)
				{}
				ps=conn.prepareStatement("SELECT ml_like FROM maplike LEFT JOIN allmaps ON am_id=ml_amid WHERE ml_amid=? AND (ml_userid=? OR ml_rid=? OR (ml_ip=? AND ml_time>?))");
				ps.setInt(1, mapid);
				ps.setInt(2, userid);
				ps.setInt(3, urid);
				ps.setString(4, ip);
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.DAY_OF_YEAR, -2);
				ps.setTimestamp(5, new java.sql.Timestamp(cal.getTimeInMillis()));
				rs=ps.executeQuery();
				byte oldlike=0;
				while (rs.next()) {
					oldlike=rs.getByte(1);
					if (oldlike!=0) break;
				}
				rs.close();
				ps.close();
				if (oldlike==0) {//new like
					ps=conn.prepareStatement("INSERT INTO maplike (ml_amid, ml_userid, ml_like, ml_rid, ml_time, ml_ip) VALUES (?, ?, ?, ?, NOW(), ?)");
					ps.setInt(1, mapid);
					ps.setInt(2, userid);
					ps.setByte(3, (byte)like);
					ps.setInt(4, urid);
					ps.setString(5, ip);
					ps.executeUpdate();
					ps.close();
				}else{//change like
					ps=conn.prepareStatement("UPDATE maplike SET ml_like=?, ml_rid=?, ml_time=NOW(), ml_ip=? WHERE ml_amid=? AND ml_userid=?");
					ps.setByte(1, (byte)like);
					ps.setInt(2, urid);
					ps.setString(3, ip);
					ps.setInt(4, mapid);
					ps.setInt(5, userid);
					int res=ps.executeUpdate();
					ps.close();
					if (res<=0) {
						ps=conn.prepareStatement("UPDATE maplike SET ml_like=?, ml_userid=?, ml_time=NOW(), ml_ip=? WHERE ml_amid=? AND ml_rid=?");
						ps.setByte(1, (byte)like);
						ps.setInt(2, userid);
						ps.setString(3, ip);
						ps.setInt(4, mapid);
						ps.setInt(5, urid);
						res=ps.executeUpdate();
						ps.close();
						if (res<=0) {
							ps=conn.prepareStatement("UPDATE maplike SET ml_like=?, ml_userid=?, ml_time=NOW(), ml_rid=? WHERE ml_amid=? AND ml_ip=? AND ml_time>?");
							ps.setByte(1, (byte)like);
							ps.setInt(2, userid);
							ps.setInt(3, urid);
							ps.setInt(4, mapid);
							ps.setString(5, ip);
							ps.setTimestamp(6, new java.sql.Timestamp(cal.getTimeInMillis()));
							res=ps.executeUpdate();
							ps.close();
						}
					}
				}
				ps=conn.prepareStatement("SELECT (SELECT SUM(ml_like) FROM maplike WHERE ml_amid=? AND ml_like>0) AS lcount, (SELECT SUM(ml_like) FROM maplike WHERE ml_amid=? AND ml_like<0) AS dcount");
				ps.setInt(1, mapid);
				ps.setInt(2, mapid);
				rs=ps.executeQuery();
				if (rs.next()) {
					lcount=rs.getInt(1);
					dcount=-rs.getInt(2);
					rs.close();
					ps.close();
					ps=conn.prepareStatement("UPDATE allmaps SET am_lcount=?, am_dcount=? WHERE am_id=?");
					ps.setInt(1, lcount);
					ps.setInt(2, dcount);
					ps.setInt(3, mapid);
					ps.executeUpdate();
				}
			}catch (Exception e){
				log("maplike error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
				mapinfo(socket, "", mapid, userid);
			}
		}
		
		void grplist(SocketChannel socket, String tmpid, int ver){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT grp.grp_id, grp.grp_name, grp.grp_lastmsg, grppart.grppart_stat FROM grp INNER JOIN grppart ON grp.grp_id=grppart.grppart_gid WHERE grppart.grppart_uid IN (SELECT users_id FROM users WHERE users_tmpid=?)");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_grplist_");
				while (rs.next()){
					int grpid=rs.getInt(1);
					String grpname=rs.getString(2);
					int lastmsg=rs.getInt(3);
					int stat=rs.getInt(4);
					result.append('\b').append(grpid).append('\b').append(grpname).append('\b').append(lastmsg).append('\b').append(stat);
				}
				send(socket, gid, result.toString());
			}catch (Exception e){
				log("grplist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void partlist(SocketChannel socket, int grpid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				if (grpid<=0) {
					send(socket, gid, "_partlisterror_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT grppart.grppart_uid, grppart.grppart_stat, users.users_nik, users.users_wason FROM grppart LEFT JOIN users ON grppart.grppart_uid=users.users_id WHERE grppart.grppart_gid=?");
				ps.setInt(1, grpid);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_partlist_\b" + grpid);
				while (rs.next()){
					int uid=rs.getInt(1);
					int stat=rs.getInt(2);
					String nik=rs.getString(3);
					String time=converttime(rs.getTimestamp(4), true);
					result.append('\b').append(uid).append('\b').append(nik).append('\b').append(time).append('\b').append(stat);
				}
				send(socket, gid, result.toString());
			}catch (Exception e){
				log("partlist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void frndlist(SocketChannel socket, String tmpid, int ver){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT frnd.frnd_did, frnd.frnd_lastmsg, users.users_nik FROM frnd INNER JOIN users ON users.users_id=frnd.frnd_did WHERE frnd.frnd_sid IN (SELECT users_id FROM users WHERE users_tmpid=?)");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_frlist_");
				while (rs.next()){
					int resid=rs.getInt("frnd_did");
					String resnik=rs.getString("users_nik");
					int lastmsg=rs.getInt("frnd_lastmsg");
					result.append('\b').append(resid).append('\b').append(resnik).append('\b').append(lastmsg);
				}
				send(socket, gid, result.toString());
			}catch (Exception e){
				log("frndlist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void frreqlist(SocketChannel socket, String tmpid, int ver){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt("users_id");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT frreq.frreq_sid, users.users_nik FROM frreq INNER JOIN users ON users.users_id=frreq.frreq_sid WHERE frreq.frreq_did=?");
				ps.setInt(1, srvid);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_reqlist_");
				while (rs.next()){
					int resid=rs.getInt("frreq_sid");
					String resnik=rs.getString("users_nik");
					result.append('\b').append(resid).append('\b').append(resnik);
				}
				send(socket, gid, result.toString());
				rs.close();
				ps.close();
				ps=conn.prepareStatement("UPDATE users SET users_newreq=0 WHERE users_id=?");
				ps.setInt(1, srvid);
				ps.executeUpdate();
			}catch (Exception e){
				log("frreqlist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void getnews(SocketChannel socket, String lng){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT news_id, news_title, news_msg FROM news WHERE news_lng=?");
				ps.setString(1, lng);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_newslist_");
				while (rs.next()){
					int newsid=rs.getInt(1);
					String title=rs.getString(2);
					String news=rs.getString(3);
					result.append('\b').append(newsid).append('\b').append(title).append('\b').append(news);
				}
				send(socket, gid, result.toString());
			}catch (Exception e){
				log("getnews error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void getservers(SocketChannel socket){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT s_ip FROM servers WHERE s_activ=1");
				rs=ps.executeQuery();
				String result="_getsrv_";
				while (rs.next()){
					String ip=rs.getString(1);
					result=result.concat("\b".concat(ip));
				}
				send(socket, gid, result);
			}catch (Exception e){
				log("getservers error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void ltoday(SocketChannel socket){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT ltoday.lt_uid, ltoday.lt_score, users.users_nik FROM ltoday LEFT JOIN users ON ltoday.lt_uid=users.users_id");
				rs=ps.executeQuery();
				String result="_ltoday_";
				while (rs.next()){
					int uid=rs.getInt(1);
					int score=rs.getInt(2);
					String nik=rs.getString(3);
					result=result.concat("\b".concat(String.valueOf(uid)).concat("\b").concat(String.valueOf(score)).concat("\b").concat(nik));
				}
				send(socket, gid, result);
			}catch (Exception e){
				log("ltoday error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void lweek(SocketChannel socket){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT lweek.lw_uid, lweek.lw_score, users.users_nik FROM lweek LEFT JOIN users ON lweek.lw_uid=users.users_id");
				rs=ps.executeQuery();
				String result="_lweek_";
				while (rs.next()){
					int uid=rs.getInt(1);
					int score=rs.getInt(2);
					String nik=rs.getString(3);
					result=result.concat("\b".concat(String.valueOf(uid)).concat("\b").concat(String.valueOf(score)).concat("\b").concat(nik));
				}
				send(socket, gid, result);
			}catch (Exception e){
				log("lweek error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void lall(SocketChannel socket){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT lall.la_uid, lall.la_score, users.users_nik FROM lall LEFT JOIN users ON lall.la_uid=users.users_id");
				rs=ps.executeQuery();
				String result="_lall_";
				while (rs.next()){
					int uid=rs.getInt(1);
					int score=rs.getInt(2);
					String nik=rs.getString(3);
					result=result.concat("\b".concat(String.valueOf(uid)).concat("\b").concat(String.valueOf(score)).concat("\b").concat(nik));
				}
				send(socket, gid, result);
			}catch (Exception e){
				log("lall error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void frmsglist(SocketChannel socket, int did, int lastmsgid, String tmpid, int ver){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid;
			try{
				DateFormat dateFormat = new SimpleDateFormat(strDateFormat, Locale.US);
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT msg_id, msg_sid, msg_time, msg_msg FROM msg WHERE msg_id>? AND ((msg_sid=? AND msg_did=?)OR(msg_sid=? AND msg_did=?)) ORDER BY msg_id DESC LIMIT 100");
				ps.setInt(1, lastmsgid);
				ps.setInt(2, sid);
				ps.setInt(3, did);
				ps.setInt(4, did);
				ps.setInt(5, sid);
				rs=ps.executeQuery();
				String msglist="_msglist_\b"+ did;
				while (rs.next()){
					int msgid=rs.getInt(1);
					int msgsid=rs.getInt(2);
					String msgtime=dateFormat.format(rs.getTimestamp(3));
					String msg=rs.getString(4);
					msglist=msglist.concat("\b".concat(String.valueOf(msgid)).concat("\b").concat(String.valueOf(msgsid)).concat("\b").concat(msg).concat("\b").concat(msgtime));
				}
				send(socket, gid, msglist);
			}catch (Exception e){
				log("frmsglist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void grmsglist(SocketChannel socket, int grpid, int lastmsgid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				DateFormat dateFormat = new SimpleDateFormat(strDateFormat, Locale.US);
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT grpmsg.gmsg_id, grpmsg.gmsg_uid, grpmsg.gmsg_time, grpmsg.gmsg_msg, users.users_nik FROM grpmsg LEFT JOIN users ON grpmsg.gmsg_uid=users.users_id WHERE gmsg_gid=? AND gmsg_id>? ORDER BY gmsg_id DESC LIMIT 100");
				ps.setInt(1, grpid);
				ps.setInt(2, lastmsgid);
				rs=ps.executeQuery();
				String msglist="_grmsglist_\b"+ grpid;
				while (rs.next()){
					int msgid=rs.getInt(1);
					int msgsid=rs.getInt(2);
					String msgtime=dateFormat.format(rs.getTimestamp(3));
					String msg=rs.getString(4);
					String nik=rs.getString(5);
					msglist=msglist.concat("\b".concat(String.valueOf(msgid)).concat("\b").concat(String.valueOf(msgsid)).concat("\b").concat(msg).concat("\b").concat(msgtime).concat("\b").concat(nik));
				}
				send(socket, gid, msglist);
			}catch (Exception e){
				log("grmsglist error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void grsendmsg(SocketChannel socket, String tmpid, int grpid, String msg){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0;
			try{
				if (tmpid==null || tmpid.length()!=12 || msg==null || msg.length()>255) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt(1);
				}
				if (srvid==0){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				if (srvid!=sysuserid){
					ps=conn.prepareStatement("SELECT grppart_id FROM grppart WHERE grppart_gid=? AND grppart_uid=? LIMIT 1");
					ps.setInt(1, grpid);
					ps.setInt(2, srvid);
					rs=ps.executeQuery();
					if (!rs.next()){
						send(socket, gid, "_error_");
						sqlclose(rs, ps, conn);
						return;
					}
					rs.close();
					ps.close();
				}
				ps=conn.prepareStatement("INSERT INTO grpmsg (gmsg_gid, gmsg_uid, gmsg_msg) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
				ps.setInt(1, grpid);
				ps.setInt(2, srvid);
				ps.setString(3, msg);
				int res=ps.executeUpdate();
				if (res>0){
					rs=ps.getGeneratedKeys();
					int msgid;
					if (rs.next()){
						DateFormat dateFormat = new SimpleDateFormat(strDateFormat, Locale.US);
						msgid=rs.getInt(1);
						String msgtime=dateFormat.format(System.currentTimeMillis());
						rs.close();
						ps.close();
						ps=conn.prepareStatement("UPDATE grp SET grp_lastmsg=? WHERE grp_id=?");
						ps.setInt(1, msgid);
						ps.setInt(2, grpid);
						ps.executeUpdate();
						ps.close();
						if (srvid!=sysuserid) send(socket, gid, "_grmsgok_\b"+ grpid +"\b"+ msgid +"\b"+msg+"\b"+msgtime);
					}
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("grmsg error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void frsendmsg(SocketChannel socket, String tmpid, int did, String msg){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid, res;
			try{
				if (tmpid==null || tmpid.length()!=12 || msg==null || msg.length()>255) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				if (sid==did){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				try{
					ps=conn.prepareStatement("INSERT INTO msg (msg_sid, msg_did, msg_msg) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
					ps.setInt(1, sid);
					ps.setInt(2, did);
					ps.setString(3, msg);
					res=ps.executeUpdate();
				}catch(SQLIntegrityConstraintViolationException e){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				if (res>0){
					rs=ps.getGeneratedKeys();
					int msgid;
					if (rs.next()){
						DateFormat dateFormat = new SimpleDateFormat(strDateFormat, Locale.US);
						msgid=rs.getInt(1);
						String msgtime=dateFormat.format(System.currentTimeMillis());
						rs.close();
						ps.close();
						ps=conn.prepareStatement("UPDATE frnd SET frnd_lastmsg=? WHERE frnd_sid=? AND frnd_did=?");
						ps.setInt(1, msgid);
						ps.setInt(2, did);
						ps.setInt(3, sid);
						ps.executeUpdate();
						ps.close();
						send(socket, gid, "_msgok_\b"+ msgid +"\b"+ sid +"\b"+ did +"\b"+msg+"\b"+msgtime);
					}
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("frsendmsg error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void frreq(SocketChannel socket, String tmpid, int did){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int sid, maxfr, frcount=0;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_maxfr FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					sid=rs.getInt("users_id");
					maxfr=rs.getInt("users_maxfr");
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				if (sid==did || did==0){
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT count(*) FROM users WHERE users_id=?");
				ps.setInt(1, did);
				rs=ps.executeQuery();
				if (rs.next()){
					if (rs.getInt(1)<=0){
						send(socket, gid, "_error_");
						sqlclose(rs, ps, conn);
						return;
					}
				}else{
					send(socket, gid, "_error_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT frreq_id FROM frreq WHERE frreq_sid=? AND frreq_did=?");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				rs=ps.executeQuery();
				if (rs.next()){
					send(socket, gid, "_frreqtwo_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT frnd_id FROM frnd WHERE frnd_sid=? AND frnd_did=?");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				rs=ps.executeQuery();
				if (rs.next()){
					send(socket, gid, "_frreqfr_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT count(*) FROM frnd WHERE frnd_sid=?");
				ps.setInt(1, sid);
				rs=ps.executeQuery();
				if (rs.next()){
					frcount=rs.getInt(1);
				}
				if (frcount>=maxfr){
					send(socket, gid, "_frreqmax_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("UPDATE users SET users_newreq=1 WHERE users_id=?");
				ps.setInt(1, did);
				ps.executeUpdate();
				ps.close();
				ps=conn.prepareStatement("INSERT INTO frreq (frreq_sid, frreq_did) VALUES (?, ?)");
				ps.setInt(1, sid);
				ps.setInt(2, did);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_frreqok_");
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("frreq error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime())+" did="+ did);
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void refr(String tmpid, int ver){
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("UPDATE users SET users_wason=now() WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				ps.executeUpdate();
			}catch (Exception e){
				log("refr error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}
		
		void savemap(SocketChannel socket, String tmpid, int mapusrrvid, String mapuid, String timestamp, String mapname, int gamerid, int mapw, int maph, int maplength, int oblength, String mapdata, String obdata){//save/update custom map
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int usrvid=0, mapid=0, status=0, mapuserid=0, score=0;
			byte active=0, hidden=0;
			String maptimestamp="";
			try{
				if (maplength!=mapdata.length() || oblength!=obdata.length()) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_status, users_score FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) {
					usrvid=rs.getInt(1);
					status=rs.getByte(2);
					score=rs.getInt(3);
				}
				if (usrvid==0 || usrvid!=mapusrrvid) {
					send(socket, gid, "_accerror_");
					sqlclose(rs, ps, conn);
					return;
				}
				if (score<100000) {
					send(socket, gid, "_screrror_");
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT am_id, am_timestamp, am_userid, am_active, am_hidden FROM allmaps WHERE am_mapuid=?");
				ps.setString(1, mapuid);
				rs=ps.executeQuery();
				if (rs.next()) {
					mapid=rs.getInt(1);
					maptimestamp=rs.getString(2);
					mapuserid=rs.getInt(3);
					active=rs.getByte(4);
					hidden=rs.getByte(5);
				}
				rs.close();
				ps.close();
				String ip="";
				try{
					InetSocketAddress addr=(InetSocketAddress)socket.getRemoteAddress();
					ip=String.valueOf(addr.getAddress());
				}catch(Exception ex)
				{}
				if (mapid==0){//new map
					if (status!=1) {
						ps=conn.prepareStatement("SELECT am_id FROM allmaps WHERE am_created>? AND (am_userid=? OR am_rid=? OR am_createdip=?)");
						Calendar cal = Calendar.getInstance();
						cal.add(Calendar.DAY_OF_YEAR, -1);
						ps.setTimestamp(1, new java.sql.Timestamp(cal.getTimeInMillis()));
						ps.setInt(2, usrvid);
						ps.setInt(3, gamerid);
						ps.setString(4, ip);
						rs=ps.executeQuery();
						if (rs.next()) {
							send(socket, gid, "_onemap_");
							sqlclose(rs, ps, conn);
							return;
						}
						rs.close();
						ps.close();
					}
					ps=conn.prepareStatement("SELECT am_id FROM allmaps WHERE am_mapname=? AND am_active=1");
					ps.setString(1, mapname);
					rs=ps.executeQuery();
					if (rs.next()) {
						send(socket, gid, "_mapname_");
						sqlclose(rs, ps, conn);
						return;
					}
					rs.close();
					ps.close();
					
					ps=conn.prepareStatement("INSERT INTO allmaps (am_mapuid, am_userid, am_timestamp, am_mapw, am_maph, am_rid, am_createdip, am_mapname) VALUES (?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
					ps.setString(1, mapuid);
					ps.setInt(2, usrvid);
					ps.setString(3, timestamp);
					ps.setInt(4, mapw);
					ps.setInt(5, maph);
					ps.setInt(6, gamerid);
					ps.setString(7, ip);
					ps.setString(8, mapname);
					int row=ps.executeUpdate();
					if (row<=0){
						sqlclose(rs, ps, conn);
						send(socket, gid, "_error_");
						return;
					}
					rs=ps.getGeneratedKeys();
					if (rs.next()) mapid=rs.getInt(1);
					if (mapid==0){
						sqlclose(rs, ps, conn);
						send(socket, gid, "_error_");
						return;
					}
					rs.close();
					ps.close();
					ps=conn.prepareStatement("INSERT INTO mapdata (md_amid, md_map, md_ob) VALUES (?,?,?)");
					ps.setInt(1, mapid);
					ps.setString(2, mapdata);
					ps.setString(3, obdata);
					row=ps.executeUpdate();
					if (row<=0){
						sqlclose(rs, ps, conn);
						send(socket, gid, "_error_");
						return;
					}
					send(socket, gid, "_mapsaved_"+"\b"+mapuid);
				}else{//update map
					if (mapuserid != mapusrrvid) {
						sqlclose(rs, ps, conn);
						send(socket, gid, "_accerror_");
						return;
					}
					if (active==0) {
						sqlclose(rs, ps, conn);
						send(socket, gid, "_mappovt_");
						return;
					}
					if (maptimestamp.equals(timestamp) && hidden==0){
						sqlclose(rs, ps, conn);
						send(socket, gid, "_mapupdated_"+"\b"+mapuid);
						return;
					}
					ps=conn.prepareStatement("SELECT am_id FROM allmaps WHERE am_mapname=? AND am_active=1 AND am_id!=?");
					ps.setString(1, mapname);
					ps.setInt(2, mapid);
					rs=ps.executeQuery();
					if (rs.next()) {
						send(socket, gid, "_mapname_");
						sqlclose(rs, ps, conn);
						return;
					}
					rs.close();
					ps.close();
					ps=conn.prepareStatement("UPDATE allmaps SET am_timestamp=?, am_rid=?, am_mapname=?, am_updated=NOW(), am_lastused=NOW(), am_hidden=0 WHERE am_id=?");
					ps.setString(1, timestamp);
					ps.setInt(2, gamerid);
					ps.setString(3, mapname);
					ps.setInt(4, mapid);
					int row=ps.executeUpdate();
					if (row<=0){
						sqlclose(rs, ps, conn);
						send(socket, gid, "_error_");
						return;
					}
					rs.close();
					ps.close();
					ps=conn.prepareStatement("UPDATE mapdata SET md_map=?, md_ob=? WHERE md_amid=?");
					ps.setString(1, mapdata);
					ps.setString(2, obdata);
					ps.setInt(3, mapid);
					ps.executeUpdate();
					send(socket, gid, "_mapupdated_"+"\b"+mapuid);
				}
			}catch (Exception e){
				log("savemap error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void getmap(SocketChannel socket, String mapuid){//get custom map
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int mapid=0, maplength=0, oblength=0;
			String mapdata="", obdata="";
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT am_id FROM allmaps WHERE am_mapuid=?");
				ps.setString(1, mapuid);
				rs=ps.executeQuery();
				if (rs.next()) mapid=rs.getInt(1);
				if (mapid==0){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT md_map, md_ob FROM mapdata WHERE md_amid=?");
				ps.setInt(1, mapid);
				rs=ps.executeQuery();
				if (rs.next()) {
					mapdata=rs.getString(1);
					obdata=rs.getString(2);
					if (mapdata!=null) maplength=mapdata.length();
					if (obdata!=null) oblength=obdata.length();
				}
				if (maplength==0 || oblength==0){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("UPDATE allmaps SET am_lastused=NOW() WHERE am_id=?");
				ps.setInt(1, mapid);
				ps.executeUpdate();
				String result="_mapdata_"+'\b'+mapuid+'\b'+ maplength +'\b'+ oblength +'\b'+mapdata+'\b'+obdata;
				send(socket, gid, result);
			}catch (Exception e){
				log("getmap error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void getmymaps(SocketChannel socket, String utmpid){//get my online maps
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int usrvid=0;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, utmpid);
				rs=ps.executeQuery();
				if (rs.next()) usrvid=rs.getInt(1);
				if (usrvid==0){
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT am_mapuid FROM allmaps WHERE am_active=1 AND am_hidden=0 AND am_userid=?");
				ps.setInt(1, usrvid);
				rs=ps.executeQuery();
				boolean send=false;
				StringBuilder result= new StringBuilder("_mymaps_");
				while (rs.next()) {
					String mapuid=rs.getString(1);
					result.append('\b').append(mapuid);
					send=true;
				}
				if (send) send(socket, gid, result.toString());
			}catch (Exception e){
				log("getmymaps error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void delmap(SocketChannel socket, String utmpid, String mapuid){//remove online map
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int usrvid=0;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=?");
				ps.setString(1, utmpid);
				rs=ps.executeQuery();
				if (rs.next()) usrvid=rs.getInt(1);
				if (usrvid==0){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("UPDATE allmaps SET am_hidden=1 WHERE am_mapuid=? AND am_userid=?");
				ps.setString(1, mapuid);
				ps.setInt(2, usrvid);
				int row=ps.executeUpdate();
				if (row>0) {
					send(socket, gid, "_delmapok_"+'\b'+mapuid);
				}else{
					send(socket, gid, "_error_");
				}
			}catch (Exception e){
				log("delmap error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void mapdauer(String mapuid, float dauer, int usrvid){//custom map dauer
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			float olddauer;
			int dauercount;
			int mapuserid;
			try{
				dauer=dauer/1800f;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT am_dauercount, am_dauer, am_userid FROM allmaps WHERE am_mapuid=?");
				ps.setString(1, mapuid);
				rs=ps.executeQuery();
				if (rs.next()) {
					dauercount=rs.getInt(1);
					olddauer=rs.getFloat(2);
					mapuserid=rs.getInt(3);
				}else{
					sqlclose(rs, ps, conn);
					return;
				}
				if (mapuserid==usrvid) {
					sqlclose(rs, ps, conn);
					return;
				}
				rs.close();
				ps.close();
				dauer=(dauer+olddauer*dauercount)/(dauercount+1);
				dauercount++;
				if (dauercount>100) dauercount=100;
				ps=conn.prepareStatement("UPDATE allmaps SET am_dauer=?, am_dauercount=? WHERE am_mapuid=?");
				ps.setFloat(1, dauer);
				ps.setInt(2, dauercount);
				ps.setString(3, mapuid);
				ps.executeUpdate();
			}catch (Exception e){
				log("mapdauer error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void getmaptop(SocketChannel socket, int topid, int ver){//get custom map top
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				StringBuilder result= new StringBuilder("_mapstop_");
				switch (topid) {
				case 1:{//popular
					ps=conn.prepareStatement("SELECT mt_amid, am_mapuid, am_userid, users_nik, am_timestamp, am_mapw, am_maph, am_mapname, am_lcount, am_dcount, am_updated FROM maptop LEFT JOIN allmaps ON am_id=mt_amid LEFT JOIN users ON users_id=am_userid WHERE am_active=1 AND am_hidden=0 ORDER BY mt_rate DESC");
					rs=ps.executeQuery();
				}
				break;
				case 2:{//new
					ps=conn.prepareStatement("SELECT am_id, am_mapuid, am_userid, users_nik, am_timestamp, am_mapw, am_maph, am_mapname, am_lcount, am_dcount, am_updated FROM allmaps LEFT JOIN users ON users_id=am_userid WHERE am_active=1 AND am_hidden=0 AND am_created>? ORDER BY am_created DESC");
					Calendar cal = Calendar.getInstance();
		            cal.add(Calendar.DAY_OF_YEAR, -7);
		            ps.setTimestamp(1, new java.sql.Timestamp(cal.getTimeInMillis()));
					rs=ps.executeQuery();
				}
				break;
				}
				boolean send=false;
				while (rs!=null && rs.next()) {
					int mapid=rs.getInt(1);
					String mapuid=rs.getString(2);
					int userid=rs.getInt(3);
					String nik=rs.getString(4);
					String timestamp=rs.getString(5);
					int mapw=rs.getInt(6);
					int maph=rs.getInt(7);
					String mapname=rs.getString(8);
					int lcount=rs.getInt(9);
					int dcount=rs.getInt(10);
					String updated=converttime(rs.getTimestamp(11), false);
					if (ver>=9) {
						result.append('\b').append(mapid).append('\b').append(mapuid).append('\b').append(userid).append('\b').append(nik).append('\b').append(timestamp).append('\b').append(mapw).append('\b').append(maph).append('\b').append(mapname).append('\b').append(lcount).append('\b').append(dcount).append('\b').append(updated);
					}else{
						result.append('\b').append(mapid).append('\b').append(mapuid).append('\b').append(userid).append('\b').append(nik).append('\b').append(timestamp).append('\b').append(mapw).append('\b').append(maph).append('\b').append(mapname).append('\b').append(lcount).append('\b').append(dcount);
					}
					send=true;
				}
				if (send) send(socket, gid, result.toString());
			}catch (Exception e){
				log("getmaptop error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void findmap(SocketChannel socket, String fmapname, int fmapid, int fmapuserid, int ver){//find custom map
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				StringBuilder result= new StringBuilder("_mapfind_");
				ps=conn.prepareStatement("SELECT am_id, am_mapuid, am_userid, users_nik, am_timestamp, am_mapw, am_maph, am_mapname, am_lcount, am_dcount, am_updated FROM allmaps LEFT JOIN users ON users_id=am_userid WHERE am_active=1 AND am_hidden=0 AND ((am_mapname LIKE ? OR ?='%%') AND (am_id=? OR ?=0) AND (am_userid=? OR ?=0)) ORDER BY am_updated DESC LIMIT 100");
				ps.setString(1, "%"+fmapname+"%");
				ps.setString(2, "%"+fmapname+"%");
				ps.setInt(3, fmapid);
				ps.setInt(4, fmapid);
				ps.setInt(5, fmapuserid);
				ps.setInt(6, fmapuserid);
				rs=ps.executeQuery();
				boolean find=false;
				while (rs!=null && rs.next()) {
					int mapid=rs.getInt(1);
					String mapuid=rs.getString(2);
					int userid=rs.getInt(3);
					String nik=rs.getString(4);
					String timestamp=rs.getString(5);
					int mapw=rs.getInt(6);
					int maph=rs.getInt(7);
					String mapname=rs.getString(8);
					int lcount=rs.getInt(9);
					int dcount=rs.getInt(10);
					String updated=converttime(rs.getTimestamp(11), false);
					if (ver>=9) {
						result.append('\b').append(mapid).append('\b').append(mapuid).append('\b').append(userid).append('\b').append(nik).append('\b').append(timestamp).append('\b').append(mapw).append('\b').append(maph).append('\b').append(mapname).append('\b').append(lcount).append('\b').append(dcount).append('\b').append(updated);
					}else{
						result.append('\b').append(mapid).append('\b').append(mapuid).append('\b').append(userid).append('\b').append(nik).append('\b').append(timestamp).append('\b').append(mapw).append('\b').append(maph).append('\b').append(mapname).append('\b').append(lcount).append('\b').append(dcount);
					}
					
					find=true;
				}
				if (find) {
					send(socket, gid, result.toString());
				}else{
					send(socket, gid, "_nomapfound_");
				}
			}catch (Exception e){
				log("findmap error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void find(SocketChannel socket, int srvid, String fnik, String fid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				if (fid==null || fid.length()<=0){//poisk po niku
					if (fnik==null){
						send(socket, gid, "_nofound_");
						sqlclose(rs, ps, conn);
						return;
					}
					ps=conn.prepareStatement("SELECT users_id, users_nik, users_wason FROM users WHERE users_nik LIKE ? AND users_id!=? ORDER BY users_wason DESC LIMIT 50");
					ps.setString(1, "%"+fnik+"%");
					ps.setInt(2, srvid);
                }else{//poisk po id
					int ifid=0;
					try {
						ifid=Integer.parseInt(fid);
					}catch (NumberFormatException e){
					}
					if (ifid==srvid || ifid==0){
						send(socket, gid, "_nofound_");
						sqlclose(rs, ps, conn);
						return;
					}
					ps=conn.prepareStatement("SELECT users_id, users_nik, users_wason FROM users WHERE users_id=?");
					ps.setInt(1, ifid);
                }
                rs=ps.executeQuery();
                boolean res=false;
				StringBuilder result= new StringBuilder("_resfound_");
				while (rs.next()){
					int resid=rs.getInt(1);
					if (resid==sysuserid) continue;
					res=true;
					String resnik=rs.getString(2);
					String restime=converttime(rs.getTimestamp(3), true);				
					result.append('\b').append(resid).append('\b').append(resnik).append('\b').append(restime);
				}
				if (res) {
					send(socket, gid, result.toString());
				}else{
					send(socket, gid, "_nofound_");
				}
			}catch (Exception e){
				log("find error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		String converttime(Timestamp ts, boolean online) {
			DateFormat dateFormat = new SimpleDateFormat(strDateFormat, Locale.US);
			long diff=System.currentTimeMillis()-ts.getTime();
            diff=Math.round((float)diff/(1000*60));
            if (diff>=0 && diff<=4 && online) {
                return "online";
            }else{
            	return dateFormat.format(ts);
            }
		}
		
		void newgrp(SocketChannel socket, String tmpid, String name){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid=0, grcount=0, maxgr=10, grpid=0, score=0;
			try{
				if (name==null || name.length()>15 || tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_error_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_maxgr, users_score FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()) {
					srvid=rs.getInt(1);
					maxgr=rs.getInt(2);
					score=rs.getInt(3);
				}
				if (srvid==0 || score<100000){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT count(*) FROM grppart WHERE grppart_uid=? AND grppart_stat=1");
				ps.setInt(1, srvid);
				rs=ps.executeQuery();
				if (rs.next()) grcount=rs.getInt(1);
				if (grcount>=maxgr){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_grpmax_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("INSERT INTO grp (grp_name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
				ps.setString(1, name);
				int row=ps.executeUpdate();
				if (row<=0){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs=ps.getGeneratedKeys();
				if (rs.next()) grpid=rs.getInt(1);
				if (grpid==0){
					sqlclose(rs, ps, conn);
					send(socket, gid, "_error_");
					return;
				}
				rs.close();
				ps.close();
				ps=conn.prepareStatement("INSERT INTO grppart (grppart_gid, grppart_uid, grppart_stat) VALUES (?, ?, 1)");
				ps.setInt(1, grpid);
				ps.setInt(2, srvid);
				row=ps.executeUpdate();
				if (row<=0){
					send(socket, gid, "_error_");
					ps.close();
					ps=conn.prepareStatement("DELETE FROM grp WHERE grp_id=?");
					ps.setInt(1, grpid);
					ps.executeUpdate();
					sqlclose(rs, ps, conn);
					return;
				}
				send(socket, gid, "_grpok_\b"+ grpid +"\b"+name);
			}catch (Exception e){
				log("newgrp error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void stat(String tmpid, int score, int enemk, int bugk, int zomk, int jugk, int ghostk, int death){
			if (score>20000 || jugk>1 || zomk>15) return;
			if (score>enemk*100+bugk*15+zomk*200+jugk*300+ghostk*250+700) return;
			if (death<0) return;
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				if (tmpid==null || tmpid.length()!=12) return;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("UPDATE users SET users_score=users_score+?, users_enemk=users_enemk+?, users_bugk=users_bugk+?, users_zomk=users_zomk+?, users_jugk=users_jugk+?, users_ghostk=users_ghostk+?, users_death=users_death+? WHERE users_tmpid=?");
				ps.setInt(1, score);
				ps.setInt(2, enemk);
				ps.setInt(3, bugk);
				ps.setInt(4, zomk);
				ps.setInt(5, jugk);
				ps.setInt(6, ghostk);
				ps.setInt(7, death);
				ps.setString(8, tmpid);
				int row=ps.executeUpdate();
				if (score!=0 && row>0){
					ps.close();
					ps=conn.prepareStatement("INSERT INTO score (score_uid, score_score) VALUES (?, ?)");
					ps.setString(1, tmpid);
					ps.setInt(2, score);
					ps.executeUpdate();
				}
			}catch (Exception e){
				log("stat error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}
		
		void addnews(String title, String lng, String news){//dobavit novost v basu
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("INSERT INTO news (news_title, news_lng, news_msg) VALUES (?, ?, ?)");
				ps.setString(1, title);
				ps.setString(2, lng);
				ps.setString(3, news);
				ps.executeUpdate();
				log("news added - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}catch (Exception e){
				log("addnews error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}

		void chpass(SocketChannel socket, String login, String oldpass, String newpass){
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				if (oldpass==null || newpass==null || login==null || login.length()>15 || newpass.length()>15) {
					send(socket, gid, "_chpasserror_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("UPDATE users SET users_pass=? WHERE users_login=? AND users_pass=?");
				ps.setString(1, newpass);
				ps.setString(2, login);
				ps.setString(3, oldpass);
				int res=ps.executeUpdate();
				if (res>0){
					send(socket, gid, "_chpassok_\b"+newpass);
				}else{
					send(socket, gid, "_chpasserror_");
				}
			}catch (Exception e){
				log("chpass error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}

		void deltmpacc(SocketChannel socket, String tmpid){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid;
			try{
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_delaccerror_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_tmpid=? AND users_login IS NULL");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt("users_id");
					rs.close();
					ps.close();
					ps=conn.prepareStatement("DELETE FROM users WHERE users_id=?");
					ps.setInt(1, srvid);
					int res=ps.executeUpdate();
					if (res>0){
						send(socket, gid, "_delaccok_");
						ps.close();
						addfreeid(conn, srvid);
					}else{
						send(socket, gid, "_delaccerror_");
					}
				}else{
					send(socket, gid, "_delaccerror_");
				}
			}catch (Exception e){
				log("deltmpacc error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		static void addfreeid(Connection conn, int srvid) throws SQLException {
			PreparedStatement ps=conn.prepareStatement("INSERT INTO freeuids (fuid_userid) VALUES (?)");
			ps.setInt(1, srvid);
			ps.executeUpdate();
			ps.close();
		}

		void delacc(SocketChannel socket, String login, String pass){
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			int srvid;
			try{
				if (pass==null || login==null || login.length()>15) {
					send(socket, gid, "_delaccerror_");
					return;
				}
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_login=? AND users_pass=?");
				ps.setString(1, login);
				ps.setString(2, pass);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt("users_id");
					rs.close();
					ps.close();
					ps=conn.prepareStatement("DELETE FROM users WHERE users_id=?");
					ps.setInt(1, srvid);
					int res=ps.executeUpdate();
					if (res>0){
						send(socket, gid, "_delaccok_");
						ps.close();
						addfreeid(conn, srvid);
					}else{
						send(socket, gid, "_delaccerror_");
					}
				}else{
					send(socket, gid, "_delaccerror_");
				}
			}catch (Exception e){
				log("delacc error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void newnik(String tmpid, String nik){
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				if (nik==null || nik.length()>15) nik="Player";
				if (tmpid==null || tmpid.length()!=12) return;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("UPDATE users SET users_nik=? WHERE users_tmpid=?");
				ps.setString(1, nik);
				ps.setString(2, tmpid);
				ps.executeUpdate();
			}catch (Exception e){
				log("newnik error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}
		
		void setnogr(String tmpid, boolean nogr){
			PreparedStatement ps=null;
			Connection conn=null;
			try{
				if (tmpid==null || tmpid.length()!=12) return;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("UPDATE users SET users_nogr=? WHERE users_tmpid=?");
				if (nogr){
					ps.setInt(1, 1);
				}else{
					ps.setInt(1, 0);
				}
				ps.setString(2, tmpid);
				ps.executeUpdate();
			}catch (Exception e){
				log("setnogr error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(null, ps, conn);
			}
		}

		void register(SocketChannel socket, String tmpid, String login, String pass, String nik){
			int srvid;
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				if (pass==null || login==null || login.length()>15) {
					send(socket, gid, "_regerror_");
					return;
				}
				if (tmpid==null || tmpid.length()!=12) {
					send(socket, gid, "_regerror_");
					return;
				}
				if (nik==null || nik.length()>15) nik="Player";
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_nik, users_login FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt("users_id");
					String oldlogin=rs.getString("users_login");
					if (!nik.equals(rs.getString("users_nik"))){
						ps.close();
						rs.close();
						ps=conn.prepareStatement("UPDATE users SET users_nik=? WHERE users_id=?");
						ps.setString(1, nik);
						ps.setInt(2, srvid);
						ps.executeUpdate();
					}
					rs.close();
					ps.close();
					ps=conn.prepareStatement("UPDATE users SET users_wason=now() WHERE users_id=?");
					ps.setInt(1, srvid);
					ps.executeUpdate();
					ps.close();
					if (oldlogin!=null){
						send(socket, gid, "_regerror_");
						sqlclose(rs, ps, conn);
						return;
					}
					ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_login=?");
					ps.setString(1, login);
					rs=ps.executeQuery();
					if (rs.next()) {
						send(socket, gid, "_regloginerror_");
						sqlclose(rs, ps, conn);
						return;
					}
					rs.close();
					ps.close();
					ps=conn.prepareStatement("UPDATE users SET users_login=?, users_pass=? WHERE users_id=?");
					ps.setString(1, login);
					ps.setString(2, pass);
					ps.setInt(3, srvid);
					int res=ps.executeUpdate();
					if (res>0){
						send(socket, gid, "_regok_");
					}else{
						send(socket, gid, "_regerror_");
					}
				}else{
					send(socket, gid, "_regerror_");
				}
			}catch (Exception e){
				log("register error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void connlogin(SocketChannel socket, int ver, String tmpid, String login, String pass, String lng, long appCode){
			int srvid, score, enemk, bugk, zomk, jugk, ghostk, death, newreq, status, nogr;
			String nik;
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				if (pass==null || login==null || login.length()>15) {
					send(socket, gid, "_connloginerror_");
					return;
				}
				String sip="";
				try{
					InetSocketAddress addr=(InetSocketAddress)socket.getRemoteAddress();
					sip=String.valueOf(addr.getAddress());
				}catch(Exception ex)
				{}
				if (checkApp(appCode, sip) <= 0) return;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_tmpid, users_nik, users_score, users_enemk, users_bugk, users_zomk, users_jugk, users_ghostk, users_death, users_newreq, users_status, users_nogr, users_created FROM users WHERE users_login=? AND users_pass=?");
				ps.setString(1, login);
				ps.setString(2, pass);
				rs=ps.executeQuery();
				if (rs.next()){
					srvid=rs.getInt("users_id");
					score=rs.getInt("users_score");
					enemk=rs.getInt("users_enemk");
					bugk=rs.getInt("users_bugk");
					zomk=rs.getInt("users_zomk");
					jugk=rs.getInt("users_jugk");
					ghostk=rs.getInt("users_ghostk");
					death=rs.getInt("users_death");
					newreq=rs.getByte("users_newreq");
					status=rs.getByte("users_status");
					nogr=rs.getByte("users_nogr");
					nik=rs.getString("users_nik");
					tmpid=rs.getString("users_tmpid");
					String registered=converttime(rs.getTimestamp("users_created"), false);
					if (nik==null || nik.length()>15) nik="Player";
					if (tmpid==null || tmpid.length()!=12) tmpid="0";
					ps.close();
					rs.close();
					ps=conn.prepareStatement("UPDATE users SET users_wason=now(), users_lastip=? WHERE users_id=?");
					ps.setString(1, sip);
					ps.setInt(2, srvid);
					ps.executeUpdate();
					send(socket, gid, "_connloginok_\b"+ srvid +'\b'+tmpid+'\b'+ score +'\b'+ enemk +'\b'+ bugk +'\b'+ zomk +'\b'+ jugk +'\b'+ ghostk +'\b'+ death +'\b'+nik+'\b'+ newreq +'\b'+ status +'\b'+ nogr +'\b'+registered);
					sqlclose(rs, ps, conn);
					lastmsgid(socket, srvid, lng);
				}else{
					send(socket, gid, "_connloginerror_");
				}
			}catch (Exception e){
				log("connlogin error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
		
		void lastmsgid(SocketChannel socket, int srvid, String lng){//id poslednih soobchenii dlja clienta
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			try{
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT frnd_did, frnd_lastmsg FROM frnd WHERE frnd_sid=?");
				ps.setInt(1, srvid);
				rs=ps.executeQuery();
				StringBuilder result= new StringBuilder("_lastfrmsg_");
				boolean res=false;
				while (rs.next()){
					res=true;
					int did=rs.getInt(1);
					int lastmsg=rs.getInt(2);
					result.append("\b").append(did).append("\b").append(lastmsg);
				}
				if (res) send(socket, gid, result.toString());
				rs.close();
				ps.close();
				res=false;
				ps=conn.prepareStatement("SELECT grp.grp_id, grp.grp_lastmsg FROM grp INNER JOIN grppart ON grp.grp_id=grppart.grppart_gid WHERE grppart.grppart_uid=?");
				ps.setInt(1, srvid);
				rs=ps.executeQuery();
				result = new StringBuilder("_lastgrmsg_");
				while (rs.next()){
					res=true;
					int grpid=rs.getInt(1);
					int lastmsg=rs.getInt(2);
					result.append("\b").append(grpid).append("\b").append(lastmsg);
				}
				if (res) send(socket, gid, result.toString());
				rs.close();
				ps.close();
				ps=conn.prepareStatement("SELECT news_id FROM news WHERE news_lng=? ORDER BY news_id DESC LIMIT 1");
				ps.setString(1, lng);
				rs=ps.executeQuery();
				if (rs.next()) send(socket, gid, "_lastnews_\b"+ rs.getInt(1));
			}catch (Exception e){
				log("lastmsgid error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}

		void conntmp(SocketChannel socket, int ver, String tmpid, String nik, String lng, long appCode){
			int srvid=0, score=0, enemk=0, bugk=0, zomk=0, jugk=0, ghostk=0, death=0, newreq=0, status=0, nogr=0;
			String registered="";
			PreparedStatement ps=null;
			ResultSet rs=null;
			Connection conn=null;
			boolean res=false;
			try{
				if (tmpid==null || tmpid.length()!=12) return;
				if (nik==null || nik.length()>15) nik="Player";
				String sip="";
				try{
					InetSocketAddress addr=(InetSocketAddress)socket.getRemoteAddress();
					sip=String.valueOf(addr.getAddress());
				}catch(Exception ex)
				{}
				if (checkApp(appCode, sip) <= 0) return;
				conn=dbpool.getConnection();
				ps=conn.prepareStatement("SELECT users_id, users_login, users_nik, users_score, users_enemk, users_bugk, users_zomk, users_jugk, users_ghostk, users_death, users_newreq, users_status, users_nogr, users_created FROM users WHERE users_tmpid=?");
				ps.setString(1, tmpid);
				rs=ps.executeQuery();
				if (rs.next()){//old user
					String login=rs.getString("users_login");
					if (login!=null && !login.isEmpty()) {
						send(socket, gid, "_connloginerror_");
						return;
					}
					srvid=rs.getInt("users_id");
					score=rs.getInt("users_score");
					enemk=rs.getInt("users_enemk");
					bugk=rs.getInt("users_bugk");
					zomk=rs.getInt("users_zomk");
					jugk=rs.getInt("users_jugk");
					ghostk=rs.getInt("users_ghostk");
					death=rs.getInt("users_death");
					newreq=rs.getByte("users_newreq");
					status=rs.getByte("users_status");
					nogr=rs.getByte("users_nogr");
					registered=converttime(rs.getTimestamp("users_created"), false);
					if (!nik.equals(rs.getString("users_nik"))){
						ps.close();
						rs.close();
						ps=conn.prepareStatement("UPDATE users SET users_nik=? WHERE users_id=?");
						ps.setString(1, nik);
						ps.setInt(2, srvid);
						ps.executeUpdate();
					}
					ps.close();
					rs.close();
					ps=conn.prepareStatement("UPDATE users SET users_wason=now(), users_lastip=? WHERE users_id=?");
					ps.setString(1, sip);
					ps.setInt(2, srvid);
					ps.executeUpdate();
					res=true;
					if (log) log("old tmp user id="+ srvid +" thr id="+ gid);
				}else{//new user
					ps.close();
					rs.close();
					ps=conn.prepareStatement("SELECT bnnik FROM bannedniks WHERE bnnik=?");
					ps.setString(1, nik);
					rs=ps.executeQuery();
					if (rs.next()) {
						rs.close();
						ps.close();
						Calendar cal = Calendar.getInstance();
						cal.add(Calendar.DAY_OF_YEAR, 2);
						ps=conn.prepareStatement("INSERT INTO bannedips (bip_ip, bip_date) VALUES (?, ?)");
						ps.setString(1, sip);
						ps.setTimestamp(2, new java.sql.Timestamp(cal.getTimeInMillis()));
						ps.executeUpdate();
						ps.close();
						return;
					}
					rs.close();
					ps.close();
					int newid=0;
					ps=conn.prepareStatement("SELECT fuid_userid FROM freeuids ORDER BY RAND() LIMIT 1");
					rs=ps.executeQuery();
					if (rs.next()) newid=rs.getInt(1);
					rs.close();
					ps.close();
					if (newid!=0){
						ps=conn.prepareStatement("DELETE FROM freeuids WHERE fuid_userid=?");
						ps.setInt(1, newid);
						ps.executeUpdate();
						rs.close();
						ps.close();
						ps=conn.prepareStatement("SELECT users_id FROM users WHERE users_id=?");
						ps.setInt(1, newid);
						rs=ps.executeQuery();
						if (rs.next()) {
							log("free user id error. id="+ newid +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
							newid=0;
						}
						rs.close();
						ps.close();
					}
					if (newid==0){
						ps=conn.prepareStatement("INSERT INTO users (users_tmpid, users_nik, users_wason, users_created, users_crip) VALUES (?, ?, now(), now(), ?)", Statement.RETURN_GENERATED_KEYS);
						ps.setString(1, tmpid);
						ps.setString(2, nik);
						ps.setString(3, sip);
						int row=ps.executeUpdate();
						if (row>0){
							rs=ps.getGeneratedKeys();
							if (rs.next()) srvid=rs.getInt(1);
						}
					}else{
						ps=conn.prepareStatement("INSERT INTO users (users_id, users_tmpid, users_nik, users_wason, users_created, users_crip) VALUES (?, ?, ?, now(), now(), ?)");
						ps.setInt(1, newid);
						ps.setString(2, tmpid);
						ps.setString(3, nik);
						ps.setString(4, sip);
						int row=ps.executeUpdate();
						if (row>0) srvid=newid;
					}
				}
				if (srvid!=0) send(socket, gid, "_conntmpok_\b"+ srvid +'\b'+ score +'\b'+ enemk +'\b'+ bugk +'\b'+ zomk +'\b'+ jugk +'\b'+ ghostk +'\b'+ death +'\b'+ newreq +'\b'+ status +'\b'+ nogr +'\b'+registered);
				if (res){
					sqlclose(rs, ps, conn);
					lastmsgid(socket, srvid, lng);
				}
			}catch (Exception e){
				log("conntmp error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
			}finally{
				sqlclose(rs, ps, conn);
			}
		}
	}

	static void send(SocketChannel socket, int id, String msg) {
		if (msg==null || socket==null) return;
		msg=msg+'\0';
		synchronized (socketlist[id]) {
			try {
				socketlist[id].add(socket);
			} catch (IllegalArgumentException | NullPointerException e){
				log("send error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
				return;
			}
            synchronized (pendingData[id]) {
				LinkedList<ByteBuffer> queue = pendingData[id].get(socket);
				if (queue == null) {
					queue = new LinkedList<>();
					try{
						pendingData[id].put(socket, queue);
					} catch (IllegalArgumentException | NullPointerException e){
						log("send error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
						return;
					}
                }
				try {
					queue.add(ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8)));
				} catch (IllegalArgumentException | NullPointerException e){
					log("send error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
					return;
				}
            }
		}
		selector.wakeup();
	}
	
	static int checkUser(int srvid, byte status, String tmpid) {
		PreparedStatement ps=null;
		ResultSet rs=null;
		Connection conn=null;
		int result=1;
		try{
			conn=dbpool.getConnection();
			ps=conn.prepareStatement("SELECT users_status, users_tmpid FROM users WHERE users_id=? AND users_tmpid=? AND users_status=?");
			ps.setInt(1, srvid);
			ps.setString(2, tmpid);
			ps.setByte(3, status);
			rs=ps.executeQuery();
			if (rs.next()) {
				byte s=rs.getByte("users_status");
				String tid=rs.getString("users_tmpid");
				if (s!=status || !tid.equals(tmpid)) result=0;
			}else{
				result=0;
			}
		}catch (Exception e){
			log("checkUser error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}finally{
			sqlclose(rs, ps, conn);
		}
		return result;
	}
	
	static String decryptStr(String encrypted, String key){
        try{
            Key aesKey=new SecretKeySpec(key.getBytes(), "AES");
            Cipher cipher=Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, aesKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(encrypted)), StandardCharsets.UTF_8);
        }catch(Exception e){
        	if (log) log("decryptStr error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
        }
        return encrypted;
    }
	
	static String getKey(int gver, int cver, String tmpid){
		PreparedStatement ps=null;
		ResultSet rs=null;
		Connection conn=null;
		String key=null;
		try{
			conn=dbpool.getConnection();
			ps=conn.prepareStatement("SELECT capp_code FROM confirmapp WHERE capp_gver=? AND capp_cver=?");
			ps.setInt(1, gver);
			ps.setInt(2, cver);
			rs=ps.executeQuery();
			if (rs.next()) key=String.valueOf(rs.getLong("capp_code"));
			if (key==null) return null;
			if (key.length()>16) key=key.substring(0, 16);
            if (key.length()<16) key+=tmpid.substring(0, 16-key.length());
		}catch (Exception e){
			log("getKey error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}finally{
			sqlclose(rs, ps, conn);
		}
		return key;
	}
	
	static int checkEncrCode(String encrCode, String ip, int gver, int cver, String tmpid) {
		PreparedStatement ps=null;
		ResultSet rs=null;
		Connection conn=null;
		int result=1;
		long appCode;
		try{
			String key=getKey(gver, cver, tmpid);
			if (key==null) return 0;
			String code=decryptStr(encrCode.trim(), key);
			try {
				appCode=Long.parseLong(code);
			}catch(NumberFormatException e){
				return 0;
			}
			if (appCode==0) return 0;
			conn=dbpool.getConnection();
			ps=conn.prepareStatement("SELECT capp_code FROM confirmapp WHERE capp_code=?");
			ps.setLong(1, appCode);
			rs=ps.executeQuery();
			if (!rs.next()) result=0;
			rs.close();
			ps.close();
			ps=conn.prepareStatement("SELECT bip_ip FROM bannedips WHERE bip_ip=?");
			ps.setString(1, ip);
			rs=ps.executeQuery();
			if (rs.next()) result=-1;
		}catch (Exception e){
			log("checkEncrCode error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}finally{
			sqlclose(rs, ps, conn);
		}
		return result;
	}
	
	static int checkApp(long appCode, String ip) {
		PreparedStatement ps=null;
		ResultSet rs=null;
		Connection conn=null;
		int result=1;
		try{
			conn=dbpool.getConnection();
			ps=conn.prepareStatement("SELECT capp_code FROM confirmapp WHERE capp_code=?");
			ps.setLong(1, appCode);
			rs=ps.executeQuery();
			if (!rs.next()) result=0;
			rs.close();
			ps.close();
			ps=conn.prepareStatement("SELECT bip_ip FROM bannedips WHERE bip_ip=?");
			ps.setString(1, ip);
			rs=ps.executeQuery();
			if (rs.next()) result=-1;
		}catch (Exception e){
			log("checkApp error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}finally{
			sqlclose(rs, ps, conn);
		}
		return result;
	}

	static void log(String str) {
		System.out.println(str);
	}

	static void closeall(){//pered vihodom
		selectorthrid++;
		for (int i = 0; i < thrcount; i++) {
			workerid[i]++;
		}
		dbpool.closeConnection();
		dbpool=null;
		try{
			if (serverSocket!=null) serverSocket.close();
			if (selector!=null) selector.close();
		} catch (IOException e) {
			log("closeall error: "+ e +" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		}
		if (mainUdpSocket!=null){
			mainUdpSocket.close();
			mainUdpSocket=null;
		}
		if (mainReciver!= null) {
            Thread dummy=mainReciver;
            mainReciver=null;
            dummy.interrupt();
        }
		if (selectorThread!= null) {
			Thread dummy=selectorThread;
			selectorThread=null;
			dummy.interrupt();
		}
		for (int i = 0; i < thrcount; i++) {
			if (tworker[i]!= null) {
				Thread dummy=tworker[i];
				tworker[i]=null;
				dummy.interrupt();
			}
		}
	}
}
