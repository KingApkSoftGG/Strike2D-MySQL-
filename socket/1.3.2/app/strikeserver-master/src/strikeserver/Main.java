package strikeserver;

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
import java.nio.ByteBuffer;
//import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
	final static int version=11;//versija online servera
	final static int ipport=37491;//nachalnii ip port
	final static int sspport=37489;//strikeserver connect port
    final static int plength=1440;//max dlina paketa
    final static int maxclient=50;//max clientov na odnom servere
    final static int maxclientnobr=20;//max clientov - no battle royal
    final static int maxunit=50;//max unitov v igre
    final static int maxbug=150;//max bugs v igre
    final static int maxserver=50;//max odnovremenno rab. serverov (sozdannih igr)
    final static int maxbrserver=3;//max serverov battle royale
    final static int maxpp=30;//buffer otpravlennih paketov
    final static int maxpr=30;//buffer prinjatih paketov
    final static int ppperiod=10;//period otpravki povt. paketa (*33ms)
    final static int maxpingpak=60;//max paketov dlja rascheta pinga
    final static int pingsendperiod=10;//period otpravki pinga igrovomu serveru
    
    static int[] maxclienttr = new int[maxserver];//max. clientov dlja tekuschego resima na servere
    
    static Timer mainTimer;
    
    static DatagramSocket mainUdpSocket;
    static Thread mainReciver;
    static byte[] mainRbuf = new byte[plength];
    static DatagramPacket mainRpacket;
    
    static InetAddress[][] gameclientIP = new InetAddress[maxserver][maxclient];//ip clientov
    static int[][] gameclientport = new int[maxserver][maxclient];//port clientov
    static boolean[][] gameclientrdy = new boolean[maxserver][maxclient];//client gotov k polucheniu dannich
    static int[][] gameclientrdytimer = new int[maxserver][maxclient];//dlja otkluchenija !gameclientrdy clientov
    static int[][] gameclientpaktimer = new int[maxserver][maxclient];//dlja otkluchenija ne aktivnich clientov
    static int[][] gameclientacpakcount = new int[maxserver][maxclient];//schetchik paketov ot klienta (dlja fps-antichit)
    static int[][] gameclientacobrtimer = new int[maxserver][maxclient];//timer obrabotki fps-antichit
    static int[][] gameclientacchit = new int[maxserver][maxclient];//kol-vo saregistrirovannih previschenii fps
    static int[][] gameclientppt = new int[maxserver][maxclient];//raschetnii period otpravki povt. paketa dlja clienta (*33ms)
    static int[][] gameclientrid = new int[maxserver][maxclient];//random id clienta
    static int[][] gameclientsrvid = new int[maxserver][maxclient];//srvid clienta
    static int[][] gameclientstatus = new int[maxserver][maxclient];//status clienta
    static int[][] gameclientappcode = new int[maxserver][maxclient];//appcode clienta
    static int[][] gamereceivedpaknum = new int[maxserver][maxclient];//nomer poslednego poluchennogo paketa
    static int[][] gamesendpaknum = new int[maxserver][maxclient];//nomer poslednego otpravlennogo paketa
    static Object[] gameclientlock = new Object[maxserver];//dlja sinchronizazii gameclient
    static DatagramSocket[] gameUdpSocket = new DatagramSocket[maxserver];//game socket
    static Thread[] gameReceiver = new Thread[maxserver];//game receiver thread
    static byte[] gameReceivertimer = new byte[maxserver];//dlja ostanovki neaktivnich serverov
    static byte[][] gameRbuf = new byte[maxserver][plength];//buffer prinjatogo paketa
    static DatagramPacket[] gameRpacket = new DatagramPacket[maxserver];//prinjatii paket
    static int[] gameport = new int[maxserver];//port game servera
    static byte[] gamenmap = new byte[maxserver];//nomer karti
    static boolean[] gamecustommap = new boolean[maxserver];//custom map
    static String[] gamemapname = new String[maxserver];//custom map name
    static String[] gamemapuid = new String[maxserver];//custom mapuid
    static byte[] gamegtype = new byte[maxserver];//tip igri
    static byte[] gameversiongame = new byte[maxserver];//versija igri
    static byte[] gameversionsrv = new byte[maxserver];//versija onlineservera na cliente
    static String[] gameverstr = new String[maxserver];//versija igri - string
    static String[] gamesrvname = new String[maxserver];//imja servera
    static String[] gamesrvpass = new String[maxserver];//parol servera
    static byte[] gamesrvstatus = new byte[maxserver];//status igroka sozdavsego server
    static byte[] gameplcount = new byte[maxserver];//kol-vo igrokov na servere
    static short[] gameping = new short[maxserver];//ping do igrovogo servera (client 0)
    static int[] gamepingtimer = new int[maxserver];//timer posilki pinga
    static int[] gamepingdeltimer = new int[maxserver];//timer udalenija pinga pri nedostavke otveta
    static long[] gamepingstime = new long[maxserver];//vremja otpravki pinga
    
    static DatagramPacket[] ppmainpak = new DatagramPacket[maxpp];              //
    static int[] ppmaintimer = new int[maxpp];             //paketi s podtversdeniem dlja mainUDP
    static int[] ppmaindeltimer = new int[maxpp];          //
    static boolean[] ppmainactiv = new boolean[maxpp];     //
    static final Object ppmainlock = new Object();//dlja sinchronizazii ppmain
    
    static DatagramPacket[][][] ppgamepak = new DatagramPacket[maxserver][maxclient][maxpp];//
    static int[][][] ppgamenum = new int[maxserver][maxclient][maxpp];               //
    static int[][][] ppgametimer = new int[maxserver][maxclient][maxpp];             //paketi s podtversdeniem dlja gameUDP
    static int[][][] ppgamedeltimer = new int[maxserver][maxclient][maxpp];          //
    static boolean[][][] ppgameactiv = new boolean[maxserver][maxclient][maxpp];     //
    static Object[] ppgamelock = new Object[maxserver];//dlja sinchronizazii ppgame
    
    static DatagramPacket[][][] prgamepak = new DatagramPacket[maxserver][maxclient][maxpr];//
    static int[][][] prgamenum = new int[maxserver][maxclient][maxpr];				//buffer prinatich paketov
    static boolean[][][] prgameactiv = new boolean[maxserver][maxclient][maxpr];	//
    
    static int[][][] pingpaknum = new int[maxserver][maxclient][maxpingpak];//nomer paketa dlja ping
    static int[][][] pingpaktr = new int[maxserver][maxclient][maxpingpak];//flag poluchenija paketa
    
    static InetAddress[][] newclientip = new InetAddress[maxserver][maxclient];	//
    static int[][] newclientport = new int[maxserver][maxclient];				//ip i port dlja nowogo klienta
    static int[][] newclientrid = new int[maxserver][maxclient];				//
    static int[][] newclientdeltimer = new int[maxserver][maxclient];			//
    static int[][] newclientsrvid = new int[maxserver][maxclient];				//
    static int[][] newclientstatus = new int[maxserver][maxclient];				//
    static int[][] newclientappcode = new int[maxserver][maxclient];			//
    static Object[] newclientlock = new Object[maxserver];						//
    
    static boolean[][] unitpossend = new boolean[maxserver][maxunit];	//komponovka unit/bug position data
    static short[][] unitx = new short[maxserver][maxunit];				//
    static short[][] unity = new short[maxserver][maxunit];				//
    static short[][] unitugol = new short[maxserver][maxunit];			//
    static boolean[][] bugsend = new boolean[maxserver][maxbug];		//
    static short[][] bugx = new short[maxserver][maxbug];				//
    static short[][] bugy = new short[maxserver][maxbug];				//
    static short[][] bugugol = new short[maxserver][maxbug];			//
    static boolean[] posdatasend = new boolean[maxserver];				//
    
    static boolean[][] unitdrawmssend = new boolean[maxserver][maxunit];//komponovka drawms
    static float[][] unitdrawms = new float[maxserver][maxunit];		//
    static boolean[] drawmsdatasend = new boolean[maxserver];			//
    
    static ByteArrayOutputStream[] gdbaos = new ByteArrayOutputStream[maxserver];
    static DataOutputStream[] gddaos = new DataOutputStream[maxserver];
    static boolean[] gdsend = new boolean[maxserver];
    static boolean[] gdopen = new boolean[maxserver];
    static byte[] gdcli = new byte[maxserver];
    
    static final Object gamereceiverlock = new Object();//dlja sinchronizazii start/stop gamereceiver
    static int log=-1;//log srvid
    static boolean paketlog=false;//log rec pname / send podtver
    
    static final int maxprofilesrv=1;
    static String[] profilesrvip = new String[maxprofilesrv];

	public static void main(String[] args) {
		log("Start server time: "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
		log("Version: "+version);
		log("----------------------");
		log("Kommand list:");
		log("1. exit");
		log("2. cl - Clients list");
		log("3. log [srvid] - On/Off log");
		log("4. paketlog - On/Off paket log");
		log("5. kill [srvid] - kill game server");
		log("----------------------");
		
		profilesrvip[0]="127.0.0.1";
		
		for (int i = 0; i < maxserver; i++) {
			ppgamelock[i]=new Object();
			newclientlock[i]=new Object();
			gameclientlock[i]=new Object();
			gameport[i]=0;
			maxclienttr[i]=maxclient;
			clearconnclient(i);
			for (int c = 0; c < maxclient; c++) delclient(i, c, false);
		}
		try {
			mainUdpSocket= new DatagramSocket(ipport);
			mainUdpSocket.setReceiveBufferSize(plength*20);
			mainRpacket = new DatagramPacket(mainRbuf, mainRbuf.length);
			log("Start main UDP Socket - OK");
		} catch (IOException e) {
			timelog("Start main UDP Socket - error: %s"+e.toString());
			return;
		}
		startmaintimer();
		mainReciver = new Thread(() -> {
            mainReciver.setPriority(Thread.MAX_PRIORITY);
            log("Start main receiver thread - OK");
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
                        int rid=dais.readInt();
                        int sid=dais.readInt();
                        String cip=dais.readUTF();
                        byte result=dais.readByte();
                        if (result<=0 && sid>=0 && sid<maxserver) {
                            for (int c = 0; c < maxclient; c++) {
                                if ((result==0 && rid==newclientrid[sid][c]) || (result==-1 && cip!=null && newclientip[sid][c]!=null && cip.equals(newclientip[sid][c].toString()))) {
                                    delconnclient(sid, c, true);
                                    break;
                                }
                            }
                            for (int c = 0; c < maxclient; c++) {
                                if ((result==0 && rid==gameclientrid[sid][c]) || (result==-1 && cip!=null && gameclientIP[sid][c]!=null && cip.equals(gameclientIP[sid][c].toString()))) {
                                    stopclient(sid, c);
                                    break;
                                }
                            }
                        }
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==11){//_checkuser_
                        int srvid=dais.readInt();
                        int rid=dais.readInt();
                        byte sid=dais.readByte();
                        byte result=dais.readByte();
                        if (result==0 && sid>=0 && sid<maxserver) {
                            for (int c = 0; c < maxclient; c++) {
                                if (rid == newclientrid[sid][c]) {
                                    timelog("kill user srvid="+ srvid +" ip="+newclientip[sid][c].toString());
                                    delconnclient(sid, c, true);
                                    break;
                                }
                            }
                            for (int c = 0; c < maxclient; c++) {
                                if (rid == gameclientrid[sid][c]) {
                                    timelog("kill user srvid="+ srvid +" ip="+gameclientIP[sid][c].toString());
                                    stopclient(sid, c);
                                    break;
                                }
                            }
                        }
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==12){//_checkapp_
                        int appCode=dais.readInt();
                        int sid=dais.readInt();
                        String cip=dais.readUTF();
                        byte result=dais.readByte();
                        if (result<=0 && sid>=0 && sid<maxserver) {
                            for (int c = 0; c < maxclient; c++) {
                                if ((result==0 && appCode==newclientappcode[sid][c]) || (result==-1 && cip!=null && newclientip[sid][c]!=null && cip.equals(newclientip[sid][c].toString()))) {
                                    delconnclient(sid, c, true);
                                    break;
                                }
                            }
                            for (int c = 0; c < maxclient; c++) {
                                if ((result==0 && appCode==gameclientappcode[sid][c]) || (result==-1 && cip!=null && gameclientIP[sid][c]!=null && cip.equals(gameclientIP[sid][c].toString()))) {
                                    stopclient(sid, c);
                                    break;
                                }
                            }
                        }
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==-1){//_createonline_
                        byte gver=dais.readByte();
                        byte sver=dais.readByte();
                        byte nmap, gtype, pcount, status, cver=0;
                        if (gver>=34) cver=dais.readByte();
                        int rid, srvid=0, appCode=457256134;
                        String verstr,sname;
                        String pass, tmpid=null;
                        if (getservercount()>=maxserver){
                            sendpname(-2,ip,port);//_serverfull_
                            dais.close();
                            bais.close();
                            continue;
                        }
                        if (sver<11){
                            sendpname(-3,ip,port);//_incversion_
                            dais.close();
                            bais.close();
                            continue;
                        }
                        rid=dais.readInt();
                        nmap=dais.readByte();
                        boolean custommap;
                        String mapname="";
                        String mapuid="";
                        String encrCode="";
                        custommap=dais.readBoolean();
                        if (custommap) {
                            mapname=dais.readUTF();
                            mapuid=dais.readUTF();
                        }
                        gtype=dais.readByte();
                        pcount=dais.readByte();
                        verstr=dais.readUTF();
                        sname=dais.readUTF();
                        pass=dais.readUTF();
                        status=dais.readByte();
                        if (gver>=32) srvid=dais.readInt();
                        if (gver>=33) {
                            tmpid=dais.readUTF();
                            if (gver==33) appCode=dais.readInt();
                            if (gver>=34) encrCode=dais.readUTF();
                            if (status!=0 && srvid==0 || rid==0 || rid==-1 || srvid==2) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                        }
                        boolean res=false;
                        for (int i = 0; i < maxserver; i++) {
                            if (gameclientport[i][0]==port && gameclientIP[i][0].equals(ip)) {
                                res=true;
                                break;
                            }
                        }
                        if (res) {
                            dais.close();
                            bais.close();
                            continue;
                        }
                        if (gtype==12 && (pass==null || pass.equals("_null_")) && status!=1){
                            int brcount=0;
                            for (int i = 0; i < maxserver; i++) {
                                if (gameport[i]==0 || gameclientport[i][0]==0 || gamegtype[i]!=12 || gamesrvpass[i]!=null) continue;
                                brcount++;
                            }
                            if (brcount>=maxbrserver) {
                                sendpname(-9,ip,port);//_brlimit_
                                dais.close();
                                bais.close();
                                continue;
                            }
                        }
                        int sid=-1;
                        synchronized (gamereceiverlock){
                            for (int i = 0; i < maxserver; i++) {
                                if (gameport[i]==0){
                                    sid=i;
                                    break;
                                }
                            }
                        }
                        if (sid!=-1){
                            int gport=ipport+sid+1;
                            if (startgamethread(sid, gport, ip, verstr)){
                                if (gver>=33) {
                                    if (srvid!=0) sendcheckuserid(srvid, status, tmpid, rid, sid);
                                    if (gver==33) sendcheckappcode(appCode, sid, ip.toString());
                                    if (gver>=34) sendcheckencrcode(encrCode, sid, rid, gver, cver, tmpid, ip.toString());
                                }
                                gamenmap[sid]=nmap;
                                gamecustommap[sid]=custommap;
                                gamemapname[sid]=mapname;
                                gamemapuid[sid]=mapuid;
                                gamegtype[sid]=gtype;
                                gameport[sid]=gport;
                                gameversiongame[sid]=gver;
                                gameversionsrv[sid]=sver;
                                gameverstr[sid]=verstr;
                                gamesrvname[sid]=sname;
                                gameplcount[sid]=pcount;
                                if(pass==null || pass.equals("_null_")){
                                    gamesrvpass[sid]=null;
                                }else{
                                    gamesrvpass[sid]=pass;
                                }
                                gamesrvstatus[sid]=status;
                                addclient(ip, port, sid, 0, rid, srvid, status, appCode);
                                gameclientrdy[sid][0]=true;
                                if (gtype==12) {
                                    maxclienttr[sid]=maxclient;
                                }else{
                                    maxclienttr[sid]=maxclientnobr;
                                }
                                sendcreategame(ip,port,gport);
                            }else{
                                sendpname(-4,ip,port);//_servererror_
                            }
                        }else{
                            sendpname(-2,ip,port);//_serverfull_
                        }
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==-6){//_onlinepodtver_
                        delmainpp(ip, port);
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==-7){//_serverlist_
                        byte sver=dais.readByte();
                        sendserverlist(ip,port,sver);
                        dais.close();
                        bais.close();
                        continue;
                    }
                    dais.close();
                    bais.close();
                } catch (IOException e) {
                    timelog("Main UDP Socket: "+ e);
                } catch (NullPointerException e) {
                    timelog("Main UDP Socket error: "+ e);
                }
            }
            closeall();
        });
		mainReciver.setDaemon(true);
		mainReciver.setName("mainReciver");
		mainReciver.start();
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
					getclientlist();
					continue;
				}
				if (s.equals("paketlog")) {
					paketlog=!paketlog;
					if (paketlog){
						log("Paket log - On");
					}else{
						log("Paket log - Off");
					}
					continue;
				}
				if (s.indexOf("log ")==0) {
					StringBuilder sid= new StringBuilder();
					for (int i=4; i<s.length(); i++) {
						sid.append(s.charAt(i));
					}
					try{
						log=Integer.parseInt(sid.toString());
					} catch(NumberFormatException e){
						log("Incorrect server id!");
					}
					if (log>=0 && log<maxserver) log("Log srvid=%s - On"+String.valueOf(log));
                    else{
						log("Log - Off");
					}
					continue;
				}
				if (s.indexOf("kill ")==0){
					StringBuilder sid= new StringBuilder();
					for (int i=5; i<s.length(); i++) {
						sid.append(s.charAt(i));
					}
					try{
						int id=Integer.parseInt(sid.toString());
						stopgamereceiver(id);
					} catch(NumberFormatException e){
						log("Incorrect server id!");
					}
					continue;
				}
				log("Kommand \""+s+"\" not found.");
			} catch (IOException e) {
				timelog("Input error: %s"+e.toString());
			}
		}
	}
	
	static void sendcheckuserid(int srvid, int status, String tmpid, int rid, int sid) {
		for (int i = 0; i < maxprofilesrv; i++) {
			try{
	            ByteArrayOutputStream baos=new ByteArrayOutputStream();
	            DataOutputStream daos=new DataOutputStream(baos);
	            daos.writeByte(11);//_checkuser_
	            daos.writeInt(srvid);
	            daos.writeByte(status);
	            daos.writeUTF(tmpid);
	            daos.writeInt(rid);
	            daos.writeByte(sid);
	            daos.close();
	            final byte[] bytes=baos.toByteArray();
	            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(profilesrvip[i]), sspport);
	            mainUdpSocket.send(pak);
	        }catch (NullPointerException e){
	        	timelog("sendcheckuserid error: %s"+e.toString());
	        }
	        catch(IOException e){
	        	timelog("sendcheckuserid error: "+ e);
	        }
		}
	}
	
	static void sendcheckencrcode(String encrCode, int sid, int rid, int gver, int cver, String tmpid, String ip) {
		for (int i = 0; i < maxprofilesrv; i++) {
			try{
	            ByteArrayOutputStream baos=new ByteArrayOutputStream();
	            DataOutputStream daos=new DataOutputStream(baos);
	            daos.writeByte(10);//_checkencrcode_
	            daos.writeByte(sid);
	            daos.writeByte(gver);
	            daos.writeByte(cver);
	            daos.writeInt(rid);
	            daos.writeUTF(tmpid);
	            daos.writeUTF(encrCode);
	            daos.writeUTF(ip);
	            daos.close();
	            final byte[] bytes=baos.toByteArray();
	            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(profilesrvip[i]), sspport);
	            mainUdpSocket.send(pak);
	        }catch (NullPointerException e){
	        	timelog("sendcheckencrcode error: %s"+e.toString());
	        }
	        catch(IOException e){
	        	timelog("sendcheckencrcode error: "+ e);
	        }
		}
	}
	
	static void sendcheckappcode(int appCode, int sid, String ip) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream daos = new DataOutputStream(baos);
			daos.writeByte(12);//_checkapp_
			daos.writeInt(appCode);
			daos.writeInt(sid);
			daos.writeUTF(ip);
			daos.close();
			final byte[] bytes = baos.toByteArray();
			DatagramPacket pak = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(profilesrvip[0]), sspport);
			mainUdpSocket.send(pak);
		} catch (NullPointerException | IOException e) {
			timelog("sendcheckappcode error: " + e);
		}
    }
	
	static void getclientlist(){//spisok clientov
		try {
			int sn=0, cn=0;
			for (int i = 0; i < maxserver; i++) {
				if (gameport[i]==0 || gameclientport[i][0]==0) continue;
				sn++;
				log("Server id="+ i +" name="+gamesrvname[i]+" ver.="+gameverstr[i]);
				for (int c = 0; c < maxclienttr[i]; c++) {
					if (gameclientport[i][c]==0) continue;
					cn++;
					log("Client sid="+ i +" cid="+ c +" ip="+ gameclientIP[i][c] +" port="+ gameclientport[i][c]);
				}
			}
			log("Total servers="+ sn +" clients="+ cn);
		} catch (NullPointerException e){
			timelog("getclientlist error: "+ e);
		}
	}
	
	static void addclient(InetAddress ip, int port, int sid, int cid, int rid, int srvid, int status, int appCode){
		if (cid<0 || cid>=maxclienttr[sid] || ip==null) return;
		synchronized (gameclientlock[sid]){
			if (gameclientIP[sid][cid]!=null){
				gameclientIP[sid][cid]=ip;
				gameclientport[sid][cid]=port;
				gameclientrid[sid][cid]=rid;
				gameclientsrvid[sid][cid]=srvid;
				gameclientstatus[sid][cid]=status;
				gameclientappcode[sid][cid]=appCode;
				if (log==sid) timelog("addclient_2 sid="+ sid +" cid="+ cid +" ip="+ ip +" port="+ port);
			}else{
				gameplcount[sid]++;
				gameclientIP[sid][cid]=ip;
				gameclientport[sid][cid]=port;
				gameclientrid[sid][cid]=rid;
				gameclientsrvid[sid][cid]=srvid;
				gameclientstatus[sid][cid]=status;
				gameclientappcode[sid][cid]=appCode;
				gamereceivedpaknum[sid][cid]=-1;
				gamesendpaknum[sid][cid]=0;
				gameclientrdy[sid][cid]=false;
				gameclientrdytimer[sid][cid]=0;
				gameclientpaktimer[sid][cid]=0;
				gameclientacchit[sid][cid]=0;
				gameclientacpakcount[sid][cid]=0;
				gameclientppt[sid][cid]=ppperiod;
				clearping(sid, cid);
				if (log==sid) timelog("addclient_1 sid="+ sid +" cid="+ cid +" ip="+ ip +" port="+ port);
			}
		}
	}
	
	static void startmaintimer(){//Main timer
		mainTimer = new Timer();
		TimerTask tt = new TimerTask() {
			@Override
			public void run() {
				try {
					obrmainpptimer();
					obrgamepptimer();
					gamereceivermanager();
					clientmanager();
					obrping();
					datasendflag();
				} catch (Exception e) {
					timelog("mainTimer error: "+ e);
				}
			}
		};
		mainTimer.schedule(tt, 0, 33);
	}
	
	static void datasendflag(){//ustanovka flaga otpravki skomponovannih dannih
		for (int i = 0; i < maxserver; i++) {
			if (gameport[i]==0 || gameclientport[i][0]==0 || gameversionsrv[i]<6) continue;
			posdatasend[i]=true;
			drawmsdatasend[i]=true;
			gdsend[i]=true;
		}
	}
	
	static void sendunitdrawms(int sid){//otsilaet obschii paket drawms unitov dlja vseh clientov
		try{
			ByteArrayOutputStream baos=new ByteArrayOutputStream();
			DataOutputStream daos=new DataOutputStream(baos);
			daos.writeByte(24);//_udrawms_
			daos.writeByte(maxunit);
			if (gameversionsrv[sid]>=10) daos.writeByte(0);
			for (int t=0; t<maxclienttr[sid]; t++) {
				if (!unitdrawmssend[sid][t]) continue;
				daos.writeByte(t);
				daos.writeFloat(unitdrawms[sid][t]);
				unitdrawmssend[sid][t]=false;
			}
			daos.close();
			final byte[] bytes=baos.toByteArray();
			DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][0], gameclientport[sid][0]);
			for (int t=0; t<maxclienttr[sid]; t++) {
				if (gameclientport[sid][t]==0 || gameclientIP[sid][t]==null) continue;
				pak.setAddress(gameclientIP[sid][t]);
				pak.setPort(gameclientport[sid][t]);
				gameUdpSocket[sid].send(pak);
			}
		}catch(Exception e){
			timelog("sendunitdrawms error: "+ e);
		}
	}
	
	static void sendunitpos(int sid){//otsilaet obschii paket s posizijami unitov dlja vseh clientov
		try{
			ByteArrayOutputStream baos=new ByteArrayOutputStream();
			DataOutputStream daos=new DataOutputStream(baos);
			daos.writeByte(15);//_unitpos_
			daos.writeByte(maxunit);
			daos.writeByte(0);
			for (int t=0; t<maxclienttr[sid]; t++) {
				if (!unitpossend[sid][t]) continue;
				daos.writeByte(t);
				daos.writeShort(unitx[sid][t]);
				daos.writeShort(unity[sid][t]);
				daos.writeShort(unitugol[sid][t]);
				unitpossend[sid][t]=false;
			}
			if (gamegtype[sid]==5 || gameversionsrv[sid]>=9) {
				boolean addbug=true;
				for (int t=0; t<maxbug; t++) {
					if (!bugsend[sid][t]) continue;
					if (addbug){
						daos.writeByte(-100);
						addbug=false;
					}
					daos.writeByte(t-maxbug/2);
					daos.writeShort(bugx[sid][t]);
					daos.writeShort(bugy[sid][t]);
					daos.writeByte(bugugol[sid][t]);
					bugsend[sid][t]=false;
				}
			}
			daos.close();
			final byte[] bytes=baos.toByteArray();
			DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][0], gameclientport[sid][0]);
			for (int t=0; t<maxclienttr[sid]; t++) {
				if (gameclientport[sid][t]==0 || gameclientIP[sid][t]==null) continue;
				pak.setAddress(gameclientIP[sid][t]);
				pak.setPort(gameclientport[sid][t]);
				gameUdpSocket[sid].send(pak);
			}
		}catch(Exception e){
			timelog("sendunitpos error: "+ e);
		}
	}
	
	static void sendgamedata(int sid){//otsilaet obschii paket s gamedata dlja vseh clientov
		try{
			byte[] data=gdbaos[sid].toByteArray();
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (gameclientport[sid][i]==0 || gameclientIP[sid][i]==null || !gameclientrdy[sid][i] || i==gdcli[sid]) continue;
				int paknum=gamesendpaknum[sid][i];
				byte[] buf=ByteBuffer.allocate(4).putInt(paknum).array();
				data[3]=buf[0];
				data[4]=buf[1];
				data[5]=buf[2];
				data[6]=buf[3];
				if (gameclientIP[sid][i]==null) continue;
				DatagramPacket pak = new DatagramPacket(data, data.length, gameclientIP[sid][i], gameclientport[sid][i]);
				gameUdpSocket[sid].send(pak);
				if (paknum!=-1) addgamepp(sid, i, data, data.length, gameclientIP[sid][i], gameclientport[sid][i], paknum);
				gamesendpaknum[sid][i]++;
			}
			gddaos[sid].close();
			gdbaos[sid].close();
		}catch(Exception e){
			timelog("sendgamedata error: "+ e);
		}
	}
	
	static void clientmanager(){//udaljaet savisschih pri sagruske clientov
		try {
			for (int i = 0; i < maxserver; i++) {
				if (gameport[i]==0 || gameclientport[i][0]==0) continue;
				for (int nc = 0; nc < maxclienttr[i]; nc++) {
					if (newclientport[i][nc]==0 || newclientip[i][nc]==null) continue;
					newclientdeltimer[i][nc]++;
					if (newclientdeltimer[i][nc]>250) delconnclient(i, nc, true);
				}
				for (int c = 0; c < maxclienttr[i]; c++) {
					if (gameclientport[i][c]==0 || gameclientIP[i][c]==null) continue;
					if (gameclientrdy[i][c] || gameclientpaktimer[i][c]<31) gameclientpaktimer[i][c]++;
					gameclientacobrtimer[i][c]++;
					if (gameclientacobrtimer[i][c]>30){
						gameclientacobrtimer[i][c]=0;
						if (gameclientacpakcount[i][c]>40){
							gameclientacchit[i][c]++;
						}else{
							if (gameclientacchit[i][c]>0) gameclientacchit[i][c]--;
						}
						gameclientacpakcount[i][c]=0;
						if (gameclientacchit[i][c]>10) stopclient(i, c);
					}
					if (c==0){
						gamepingtimer[i]++;
						if (gamepingtimer[i]>pingsendperiod){
							if (gamepingstime[i]==0){
								gamepingtimer[i]=0;
								gamepingdeltimer[i]=0;
								if (sendgamepname(i, -10, gameclientIP[i][0], gameclientport[i][0])){//_ping_
									gamepingstime[i]=System.currentTimeMillis();
								}
							}else{
								gamepingdeltimer[i]++;
								if (gamepingdeltimer[i]>33){
									gamepingdeltimer[i]=0;
									gamepingstime[i]=0;
								}
							}
						}
					}
					if (gameclientpaktimer[i][c]>250){
						stopclient(i, c);
						continue;
					}
					if (gameclientrdy[i][c]){
						gameclientrdytimer[i][c]=0;
					}else{
						gameclientrdytimer[i][c]++;
						if (gameclientrdytimer[i][c]>1000) {
							if (log==i) timelog("_5_delclient sid="+ i +" cid="+ c);
							delclient(i,c,true);
						}
					}
				}
			}
		} catch (NullPointerException e){
			timelog("clientmanager error: "+ e);
		}
	}
	
	static void stopclient(int sid, int cid){//udalenie clienta s perenasnacheniem servera
		if (cid==0) {
			if (clienttogameserver(sid)==-1) stopgamereceiver(sid);
		}else{
			delclient(sid, cid, true);
		}
	}
	
	static void gamereceivermanager(){//zakrivaet neispolsuemii gameReciver
		for (int i = 0; i < maxserver; i++) {
			if (gameport[i]!=0) gameReceivertimer[i]++;
			if (gameReceivertimer[i]>250){
				gameReceivertimer[i]=0;
				if (log==i) timelog("_5_stopgamereceiver id="+i);
				stopgamereceiver(i);
			}
		}
	}
	
	static void stopgamereceiver(int sid){
		if (gameversionsrv[sid]<3){
			for (int i = 1; i < maxclienttr[sid]; i++) {
				if (gameclientIP[sid][i]!=null && gameclientport[sid][i]!=0) sendgamepname(sid, -9, gameclientIP[sid][i], gameclientport[sid][i]);//_stopserver_
			}
		}
		synchronized (gamereceiverlock){
			try {
				if (gameUdpSocket[sid]!=null){
					gameUdpSocket[sid].close();
					gameUdpSocket[sid]=null;
				}
				if (gameReceiver[sid]!=null) {
					Thread dummy=gameReceiver[sid];
					gameReceiver[sid]=null;
					dummy.interrupt();
				}
				gameport[sid]=0;
				Arrays.fill(unitpossend[sid], false);
				Arrays.fill(bugsend[sid], false);
				clearconnclient(sid);
				for (int c = 0; c < maxclienttr[sid]; c++) delclient(sid, c, true);
				gameplcount[sid]=0;
				gdopen[sid]=false;
				maxclienttr[sid]=maxclientnobr;
				try {
					if (gddaos[sid]!=null) {
						gddaos[sid].close();
						gddaos[sid]=null;
					}
					if (gdbaos[sid]!=null) {
						gdbaos[sid].close();
						gdbaos[sid]=null;
					}
				}catch (IOException e) {

				}
			} catch (NullPointerException e){
				timelog("stopgamereceiver error: "+ e);
			}
		}
	}
	
	static int getservercount(){//kol-vo aktivnih serverov
		int count=0;
		for (int i = 0; i < maxserver; i++) {
			if (gameport[i]!=0 && gameclientport[i][0]!=0) count++;
		}
		return count;
	}

	static void delclient(int sid, int cid, boolean send){
		if (cid<0 || cid>=maxclient) return;
		if (gameclientIP[sid][cid]!=null){
			gameplcount[sid]--;
			if (cid!=0 && gameclientIP[sid][0]!=null && send) {
				senddisconnect(sid, cid, gameclientrid[sid][cid]);
			}
		}
		synchronized (gameclientlock[sid]){
			if (sid==log) timelog("delclient cid="+ cid);
			gameclientIP[sid][cid]=null;
			gameclientport[sid][cid]=0;
			gamereceivedpaknum[sid][cid]=-1;
			gamesendpaknum[sid][cid]=0;
			gameclientrdy[sid][cid]=false;
			gameclientrdytimer[sid][cid]=0;
			gameclientpaktimer[sid][cid]=0;
			gameclientacchit[sid][cid]=0;
			gameclientacpakcount[sid][cid]=0;
			gameclientrid[sid][cid]=0;
			gameclientsrvid[sid][cid]=0;
			gameclientstatus[sid][cid]=0;
			gameclientappcode[sid][cid]=0;
			clearping(sid, cid);
			cleargamepp(sid, cid);
			cleargamepr(sid, cid);
		}
	}
	
	static void timelog(String msg){
		System.out.println(msg+" - "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime()));
	}
	
	static void log(String msg){
		System.out.println(msg);
	}
	
	static void senddisconnect(int sid, int cid, int rid){//udalenie clienta na igrovom servere
		try{
			if (gameUdpSocket[sid]==null) return;
			ByteArrayOutputStream baos=new ByteArrayOutputStream();
			DataOutputStream daos=new DataOutputStream(baos);
			daos.writeByte(17);//_disconn_
			daos.writeByte(0);
			daos.writeByte(cid);
			daos.writeInt(rid);
			daos.close();
			final byte[] bytes=baos.toByteArray();
			DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][0], gameclientport[sid][0]);
			gameUdpSocket[sid].send(pak);
		}catch (NullPointerException | IOException e){
			timelog("senddisconnect sid="+ sid +" error: "+ e);
		}
	}
	
	static void closeall(){
		if (mainTimer!=null){
			mainTimer.cancel();
			mainTimer.purge();
			mainTimer=null;
		}
		if (mainUdpSocket!=null){
			mainUdpSocket.close();
			mainUdpSocket=null;
		}
		for (int i = 0; i < maxserver; i++) {
			if (gameUdpSocket[i]!=null){
				gameUdpSocket[i].close();
				gameUdpSocket[i]=null;
			}
		}
		if (mainReciver!= null) {
            Thread dummy=mainReciver;
            mainReciver=null;
            dummy.interrupt();
        }
		for (int i = 0; i < maxserver; i++) {
			if (gameReceiver[i]!= null) {
	            Thread dummy=gameReceiver[i];
	            gameReceiver[i]=null;
	            dummy.interrupt();
	        }
			gameport[i]=0;
			for (int c = 0; c < maxclient; c++) delclient(i, c, true);
			gameplcount[i]=0;
		}
	}
	
	static void sendgamepodtver(int sid, InetAddress ip, int port, int num){
		if (ip==null) return;
        try{
        	if (log==sid && paketlog) log("sendgamepodtver sid="+ sid +" ip="+ ip +" port="+ port +" num="+ num);
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            DataOutputStream daos=new DataOutputStream(baos);
            daos.writeByte(-6);//_onlinepodtver_
            daos.writeInt(num);
            daos.close();
            final byte[] bytes=baos.toByteArray();
            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, ip, port);
            gameUdpSocket[sid].send(pak);
        } catch (NullPointerException | IOException e){
        	timelog("sendgamepodtver rid="+ sid +" ip="+ ip +" port="+ port +" error: "+ e);
        }
    }
	
	static void addconnclient(int sid, InetAddress ip, int port, int rid, int srvid, int status, int appCode){//pri podkluchenii nowogo klienta
		if (ip==null) return;
		synchronized (newclientlock[sid]){
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (newclientrid[sid][i]==rid) {
					newclientip[sid][i]=ip;
					newclientport[sid][i]=port;
					newclientsrvid[sid][i]=srvid;
					newclientstatus[sid][i]=status;
					newclientappcode[sid][i]=appCode;
					newclientdeltimer[sid][i]=0;
					if (log==sid) timelog("addnewclient_1 rid="+ rid +" ip="+ ip +" port="+ port);
					return;
				}
			}
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (newclientport[sid][i]==0 && newclientrid[sid][i]==-1){
					newclientip[sid][i]=ip;
					newclientport[sid][i]=port;
					newclientrid[sid][i]=rid;
					newclientsrvid[sid][i]=srvid;
					newclientstatus[sid][i]=status;
					newclientappcode[sid][i]=appCode;
					newclientdeltimer[sid][i]=0;
					if (log==sid) timelog("addnewclient_2 rid="+ rid +" ip="+ ip +" port="+ port);
					return;
				}
			}
		}
	}
	
	static void delconnclient(int sid, int ncid, boolean disconnect){
		synchronized (newclientlock[sid]){
			if (disconnect && gameclientIP[sid][0]!=null) {
				senddisconnect(sid, -1, newclientrid[sid][ncid]);
			}
			newclientip[sid][ncid]=null;
			newclientport[sid][ncid]=0;
			newclientrid[sid][ncid]=-1;
			newclientsrvid[sid][ncid]=0;
			newclientstatus[sid][ncid]=0;
			newclientappcode[sid][ncid]=0;
			newclientdeltimer[sid][ncid]=0;
		}
	}
	
	static int getconnclientid(int sid, int rid){
		for (int i = 0; i < maxclienttr[sid]; i++) {
			if (newclientrid[sid][i]==rid) return i;
		}
		return -1;
	}
	
	static void clearconnclient(int sid){
		synchronized (newclientlock[sid]){
			for (int i = 0; i < maxclient; i++) {
				newclientip[sid][i]=null;
				newclientport[sid][i]=0;
				newclientrid[sid][i]=-1;
				newclientsrvid[sid][i]=0;
				newclientstatus[sid][i]=0;
				newclientappcode[sid][i]=0;
				newclientdeltimer[sid][i]=0;
			}
		}
	}
	
	static void copyclient(int sid, int clsourid, int cldestid){//kopirovanie dannich clienta v novii id
		try {
			synchronized (gameclientlock[sid]){
				gameclientIP[sid][cldestid]=gameclientIP[sid][clsourid];
				gameclientport[sid][cldestid]=gameclientport[sid][clsourid];
				gameclientrid[sid][cldestid]=gameclientrid[sid][clsourid];
				gameclientsrvid[sid][cldestid]=gameclientsrvid[sid][clsourid];
				gameclientstatus[sid][cldestid]=gameclientstatus[sid][clsourid];
				gameclientappcode[sid][cldestid]=gameclientappcode[sid][clsourid];
				gamereceivedpaknum[sid][cldestid]=gamereceivedpaknum[sid][clsourid];
				gamesendpaknum[sid][cldestid]=gamesendpaknum[sid][clsourid];
				gameclientrdy[sid][cldestid]=true;
				gameclientrdytimer[sid][cldestid]=0;
				gameclientpaktimer[sid][cldestid]=0;
				gameclientacpakcount[sid][cldestid]=0;
				gameclientacchit[sid][cldestid]=0;
				for (int i=0; i<maxpingpak; i++) {
					pingpaknum[sid][cldestid][i]=pingpaknum[sid][clsourid][i];
					pingpaktr[sid][cldestid][i]=pingpaktr[sid][clsourid][i];
				}
				for (int i=0; i<maxpp; i++) {
					ppgamepak[sid][cldestid][i]=ppgamepak[sid][clsourid][i];
					ppgamenum[sid][cldestid][i]=ppgamenum[sid][clsourid][i];
					ppgamedeltimer[sid][cldestid][i]=ppgamedeltimer[sid][clsourid][i];
					ppgameactiv[sid][cldestid][i]=ppgameactiv[sid][clsourid][i];
				}
				for (int i=0; i<maxpr; i++) {
					prgamepak[sid][cldestid][i]=prgamepak[sid][clsourid][i];
					prgamenum[sid][cldestid][i]=prgamenum[sid][clsourid][i];
					prgameactiv[sid][cldestid][i]=prgameactiv[sid][clsourid][i];
				}
			}
		} catch (NullPointerException e) {
			timelog("copyclient sid="+ sid +" error: "+ e);
		}
	}
	
	static void sendnewgameserver(int sid, int newgsid){//_newgameserver_
		try {
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (gameclientport[sid][i] == 0 || gameclientIP[sid][i] == null) continue;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream daos = new DataOutputStream(baos);
				daos.writeByte(-12);
				daos.writeByte(newgsid);
				daos.close();
				final byte[] bytes = baos.toByteArray();
				DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][i], gameclientport[sid][i]);
				gameUdpSocket[sid].send(pak);
			}
		} catch (NullPointerException | IOException e) {
			timelog("sendnewgameserver sid=" + sid + " error: " + e);
		}
        try {
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (gameclientport[sid][i] == 0 || gameclientIP[sid][i] == null) continue;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream daos = new DataOutputStream(baos);
				daos.writeByte(-12);
				daos.writeByte(newgsid);
				daos.close();
				final byte[] bytes = baos.toByteArray();
				DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][i], gameclientport[sid][i]);
				gameUdpSocket[sid].send(pak);
			}
		} catch (NullPointerException | IOException e) {
			timelog("sendnewgameserver sid=" + sid + " error: " + e);
		}
        try {
			for (int i = 0; i < maxclienttr[sid]; i++) {
				if (gameclientport[sid][i] == 0 || gameclientIP[sid][i] == null) continue;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream daos = new DataOutputStream(baos);
				daos.writeByte(-12);
				daos.writeByte(newgsid);
				daos.close();
				final byte[] bytes = baos.toByteArray();
				DatagramPacket pak = new DatagramPacket(bytes, bytes.length, gameclientIP[sid][i], gameclientport[sid][i]);
				gameUdpSocket[sid].send(pak);
			}
		} catch (NullPointerException | IOException e) {
			timelog("sendnewgameserver sid=" + sid + " error: " + e);
		}
    }
	
	static int clienttogameserver(int sid){//vibor novogo game servera pri otkluchenii starogo
		if (gameversionsrv[sid]<3) return -1;
		int minping=0;
		int cid=-1;
		for (int i = 1; i < maxclienttr[sid]; i++) {
			if (gameclientIP[sid][i]!=null && gameclientport[sid][i]!=0 && gameclientrdy[sid][i]){
				if (gameclientppt[sid][i]!=0 && (gameclientppt[sid][i]<minping || cid==-1)){
					minping=gameclientppt[sid][i];
					cid=i;
				}
			}
		}
		if (cid!=-1){
			delclient(sid, 0, false);
			sendnewgameserver(sid, cid);
			copyclient(sid, cid, 0);
			delclient(sid, cid, false);
			gameplcount[sid]++;
			return cid;
		}
		return -1;
	}

	static boolean startgamethread(final int id, final int srvport, final InetAddress srvip, final String ver){//start game servera
		try {
			System.gc();
			gameping[id]=0;
		    gamepingtimer[id]=0;
		    gamepingdeltimer[id]=0;
		    gamepingstime[id]=0;
			gameUdpSocket[id]= new DatagramSocket(srvport);
			gameUdpSocket[id].setReceiveBufferSize(plength*20);
			gameUdpSocket[id].setSendBufferSize(plength*20);
			gameUdpSocket[id].setBroadcast(false);
			Arrays.fill(gameRbuf[id], (byte)0);
			gameRpacket[id] = new DatagramPacket(gameRbuf[id], plength);
			gameReceivertimer[id]=0;
			gdopen[id]=false;
		} catch (IOException | NullPointerException e) {
			timelog("Start game UDP Socket "+ id +" - error: "+ e);
			stopgamereceiver(id);
			return false;
		}
        gameReceiver[id]= new Thread(() -> {
            if (log==id) timelog("startgamereceiver sid="+ id +" cid0IP="+ srvip +" cid0Ver="+ver);
            while (gameUdpSocket[id]!=null && !gameUdpSocket[id].isClosed()) {
                try {
                    gameUdpSocket[id].receive(gameRpacket[id]);
                    gameReceivertimer[id]=0;
                    int praz=gameRpacket[id].getLength();
                    InetAddress ip=gameRpacket[id].getAddress();
                    int port=gameRpacket[id].getPort();
                    if (praz<1) continue;
                    byte[] data = gameRpacket[id].getData();
                    ByteArrayInputStream bais=new ByteArrayInputStream(data, 0, praz);
                    DataInputStream dais=new DataInputStream(bais);
                    byte pname=dais.readByte();
                    if (log==id && paketlog) log("rec pname sid="+ id +" pname="+String.valueOf(pname)+" ip="+ ip +" port="+ port);
                    byte cid=-1;
                    int num;
                    if (pname==-6){//_onlinepodtver_
                        cid=dais.readByte();
                        num=dais.readInt();
                        if (cid<0 || cid>=maxclienttr[id]) {
                            dais.close();
                            bais.close();
                            continue;
                        }
                        if (ip==null || !ip.equals(gameclientIP[id][cid])) {
                            dais.close();
                            bais.close();
                            continue;
                        }
                        addrping(id, cid, num);
                        delgamepp(id, cid, num);
                        dais.close();
                        bais.close();
                        continue;
                    }
                    if (pname==-10){//_ping_
                        gameping[id]=(short)(System.currentTimeMillis()-gamepingstime[id]);
                        gamepingstime[id]=0;
                        dais.close();
                        bais.close();
                        continue;
                    }
                    byte pnaz=dais.readByte();
                    if (pname==24 && pnaz==maxunit){//_udrawms_
                        if (gameversionsrv[id]>=10) {
                            int cl=dais.readByte();
                            if (cl>=0 && cl<maxclient) {
                                if (ip==null || !ip.equals(gameclientIP[id][cl])) {
                                    dais.close();
                                    bais.close();
                                    continue;
                                }
                            }
                        }
                        while (dais.available()>0){
                            int uid=dais.readByte();
                            float drawms=dais.readFloat();
                            if (uid<0 || uid>=maxunit) continue;
                            unitdrawms[id][uid]=drawms;
                            unitdrawmssend[id][uid]=true;
                        }
                        dais.close();
                        bais.close();
                        if (drawmsdatasend[id]){
                            sendunitdrawms(id);
                            drawmsdatasend[id]=false;
                        }
                        continue;
                    }
                    if (gameversionsrv[id]>=10 && (pname==24 || pname==25 || pname==26)) {
                        int cl=dais.readByte();
                        if (cl>=0 && cl<maxclient) {
                            if (ip==null || !ip.equals(gameclientIP[id][cl])) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                        }
                    }
                    if (pname==15){//_unitpos_
                        int cl=dais.readByte();
                        if (cl>=0 && cl<maxclient) {
                            if (ip==null || !ip.equals(gameclientIP[id][cl])) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                            if (gameclientport[id][cl]==port){
                                gameclientpaktimer[id][cl]=0;
                                gameclientacpakcount[id][cl]++;
                            }else{
                                if (gameclientpaktimer[id][cl]>30) {
                                    gameclientport[id][cl]=port;
                                    if (log==id) log("chport cid="+ cl +" port="+ port);
                                }
                            }
                        }
                        if (gameversionsrv[id]>=6){
                            int uid;
                            short ux,uy,uugol;
                            boolean unit=true;
                            while (dais.available()>0){
                                if (unit) {
                                    uid=dais.readByte();
                                    if (uid==-100){
                                        unit=false;
                                        continue;
                                    }
                                    ux=dais.readShort();
                                    uy=dais.readShort();
                                    uugol=dais.readShort();
                                    if (uid<0 || uid>=maxunit) continue;
                                    unitx[id][uid]=ux;
                                    unity[id][uid]=uy;
                                    unitugol[id][uid]=uugol;
                                    unitpossend[id][uid]=true;
                                }else{
                                    uid=dais.readByte();
                                    ux=dais.readShort();
                                    uy=dais.readShort();
                                    uugol=dais.readByte();
                                    uid=uid+maxbug/2;
                                    if (uid<0 || uid>=maxbug) continue;
                                    bugx[id][uid]=ux;
                                    bugy[id][uid]=uy;
                                    bugugol[id][uid]=uugol;
                                    bugsend[id][uid]=true;
                                }
                            }
                            dais.close();
                            bais.close();
                            if (posdatasend[id]){
                                sendunitpos(id);
                                posdatasend[id]=false;
                            }
                            continue;
                        }
                    }
                    if (pname==17){//_disconn_
                        int cl=dais.readByte();
                        int rid=0;
                        if (praz>3) rid=dais.readInt();
                        if (cl>=0 && cl<maxclienttr[id] && (rid==0 || rid==gameclientrid[id][cl])){
                            if (ip==null || !ip.equals(gameclientIP[id][cl])) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                            stopclient(id, cl);
                        }
                    }
                    if (pname!=7 && pname!=8 && pname!=15 && pname!=17 && pname!=23 && pname!=24 && pname!=25 && pname!=26){
                        cid=dais.readByte();
                        num=dais.readInt();
                        if (cid>=0 && cid<maxclienttr[id] && (ip==null || !ip.equals(gameclientIP[id][cid]))) {
                            dais.close();
                            bais.close();
                            continue;
                        }
                        if (cid>=0 && cid<maxclienttr[id] && gameclientport[id][cid]!=port && gameclientpaktimer[id][cid]>30){
                            gameclientport[id][cid]=port;
                            if (log==id) log("chport cid="+String.valueOf(cid)+" port="+ port);
                        }
                        if (pname==-8){//_plcount_
                            byte plc=dais.readByte();
                            if (cid==0 && plc>0 && plc<=maxclienttr[id]){
                                gameplcount[id]=plc;
                            }
                        }
                        if (pname==22){//_newclntrdy_
                            if (cid>=0 && cid<maxclienttr[id]) gameclientrdy[id][cid]=true;
                        }
                        if (pname==18 && gameversionsrv[id]>=13){//_disconnban_
                            int status=dais.readByte();
                            if (status==1 && cid>=0 && cid<maxclienttr[id]) {
                                if (gameclientstatus[id][cid]!=status) {
                                    stopclient(id, cid);
                                    dais.close();
                                    bais.close();
                                    continue;
                                }
                            }
                        }
                        if (pname==6){//_connect_
                            int rid=dais.readInt();
                            int gver=dais.readByte();
                            int srvid=0, status=0, appCode=976953303;
                            String tmpid=null;
                            if (gver>=24){
                                srvid=dais.readInt();//srvid
                                dais.readInt();//score
                                status=dais.readByte();//status
                                dais.readUTF();//plname
                                String pass=dais.readUTF();
                                if (gver>=33) {
                                    if (gver==33) appCode=dais.readInt();//appCode
                                    dais.readInt();//svCount
                                    tmpid=dais.readUTF();
                                }
                                if (gamesrvpass[id]!=null && (pass==null || !pass.equals(gamesrvpass[id]))){
                                    sendgamepodtver(id, ip, port, num);
                                    sendgamepname(id, -13, ip, port);//_incpass_
                                    dais.close();
                                    bais.close();
                                    continue;
                                }
                                if (srvid==0 && (status!=0 || gameclientsrvid[id][0]!=0) || rid==0 || rid==-1 || srvid==2) {
                                    sendgamepodtver(id, ip, port, num);
                                    dais.close();
                                    bais.close();
                                    continue;
                                }
                            }
                            boolean res=true;
                            if (log==id) timelog("_connect_ rid="+ rid +" ip="+ ip +" port="+ port);
                            for (int i = 1; i < maxclienttr[id]; i++) {
                                if (gameclientIP[id][i]==null && gameclientport[id][i]==0){
                                    res=false;
                                    break;
                                }
                            }
                            if (res){
                                sendgamepodtver(id, ip, port, num);
                                sendgamepname(id, -11, ip, port);//_gamefull_
                                dais.close();
                                bais.close();
                                continue;
                            }
                            if (gver>=33) {
                                if (srvid!=0) sendcheckuserid(srvid, status, tmpid, rid, id);
                                sendcheckappcode(appCode, id, ip.toString());
                            }
                            addconnclient(id, ip, port, rid, srvid, status, appCode);
                        }
                        if (pname==9 || pname==20){//_conncomm_ / _addclnt_
                            int cl=dais.readByte();
                            int rid=dais.readInt();
                            InetAddress ncip=null;
                            int ncport=0, ncid, srvid=0, status=0, appCode=0;
                            synchronized (newclientlock[id]){
                                ncid=getconnclientid(id, rid);
                                if (ncid!=-1) {
                                    ncip=newclientip[id][ncid];
                                    ncport=newclientport[id][ncid];
                                    srvid=newclientsrvid[id][ncid];
                                    status=newclientstatus[id][ncid];
                                    appCode=newclientappcode[id][ncid];
                                }
                            }
                            if (ncid!=-1 && ncport!=0 && ncip!=null){
                                addclient(ncip, ncport, id, cl, rid, srvid, status, appCode);
                                delconnclient(id, ncid, false);
                            }
                        }
                        if (pname==12){//_disconnp_
                            int cl=dais.readByte();
                            int rid=0;
                            if (praz>8) rid=dais.readInt();
                            if (cl>=0 && cl<maxclienttr[id] && (rid==0 || rid==gameclientrid[id][cl])){
                                stopclient(id, cl);
                            }
                        }
                        if (cid<0 || cid>=maxclienttr[id]){
                            sendgamepodtver(id, ip, port, num);
                            obrpaket(id, ip, port, data, praz, pnaz, cid, pname, true);
                            dais.close();
                            bais.close();
                            continue;
                        }else{
                            if (gameclientIP[id][cid]==null) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                        }
                        if (num>gamereceivedpaknum[id][cid]+1){
                            if (gamereceivedpaknum[id][cid]==-1) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                            if (addgamepr(id, cid, num, data, praz, ip, port)) {
                                sendgamepodtver(id, ip, port, num);
                            }
                            int irp=findgamepr(id, cid, gamereceivedpaknum[id][cid]+1);
                            while (irp!=-1){
                                dais.close();
                                bais.close();
                                DatagramPacket pak=prgamepak[id][cid][irp];
                                if (pak==null || pak.getLength()<=0){
                                    delgamepr(id, cid, irp);
                                    irp=findgamepr(id, cid, gamereceivedpaknum[id][cid]+1);
                                    continue;
                                }
                                byte[] prdata = pak.getData();
                                int prlength=pak.getLength();
                                bais=new ByteArrayInputStream(prdata, 0, prlength);
                                dais=new DataInputStream(bais);
                                pname=dais.readByte();
                                pnaz=dais.readByte();
                                cid=dais.readByte();
                                num=dais.readInt();
                                gamereceivedpaknum[id][cid]++;
                                obrpaket(id, gameclientIP[id][cid], gameclientport[id][cid], prdata, prlength, pnaz, cid, pname, true);
                                delgamepr(id, cid, irp);
                                irp=findgamepr(id, cid, gamereceivedpaknum[id][cid]+1);
                            }
                            dais.close();
                            bais.close();
                            continue;
                        }else{
                            sendgamepodtver(id, ip, port, num);
                            if (num<=gamereceivedpaknum[id][cid]) {
                                dais.close();
                                bais.close();
                                continue;
                            }
                            gamereceivedpaknum[id][cid]++;
                            obrpaket(id, ip, port, data, praz, pnaz, cid, pname, true);
                        }
                    }else{
                        obrpaket(id, ip, port, data, praz, pnaz, cid, pname, false);
                    }
                    dais.close();
                    bais.close();
                } catch (IOException e) {
                    if (log==id) timelog("Game UDP Socket id="+ id +" error: "+ e);
                } catch (NullPointerException e) {
                    timelog("Game UDP Socket id="+ id +" error: "+ e);
                }
            }
            if (log==id) timelog("_4_stopgamereceiver id="+ id);
            if (gameUdpSocket[id]!=null) gameUdpSocket[id].close();
        });
		gameReceiver[id].setDaemon(true);
		gameReceiver[id].setName("GameReceiver"+ id);
		gameReceiver[id].start();
        return true;
	}
	
	static void obrpaket(int sid, InetAddress ip, int port, byte[] data, int datalength, byte pnaz, byte pcli, byte pname, boolean pp){
		int paknum=-1;
		try {
			if (data==null || ip==null || (pp && data.length<7)) return;
			boolean toall=((pnaz==maxclient && gameversionsrv[sid]>=8) || ((pnaz==maxclientnobr && gameversionsrv[sid]<8)));
			if (datalength>25 && pname==21) {//_addunit_
				int tip=data[9];
				if (tip==0 || tip==1) {
					ByteBuffer bb = ByteBuffer.allocate(2);
					bb.put(data[24]);
					bb.put(data[25]);
					int life=bb.getShort(0);
					if (life>100) {
						stopclient(sid, pcli);
					}
				}
			}
			if (toall){
				if (pname==1 && gameversionsrv[sid]>=8) {//_gamedata_
					if (!gdopen[sid]) {
						gdbaos[sid]=new ByteArrayOutputStream();
						gddaos[sid]=new DataOutputStream(gdbaos[sid]);
						gddaos[sid].write(data, 0, datalength);
						gdopen[sid]=true;
						gdcli[sid]=pcli;
					}else{
						if (gdbaos[sid].size()+datalength>=plength) {
							sendgamedata(sid);
	            			gdbaos[sid]=new ByteArrayOutputStream();
							gddaos[sid]=new DataOutputStream(gdbaos[sid]);
							gddaos[sid].write(data, 0, datalength);
							gdcli[sid]=pcli;
						}else{
							gddaos[sid].writeByte(-1);
							gddaos[sid].writeByte(pcli);
							gddaos[sid].write(data, 7, datalength-7);
							if (gdcli[sid]!=pcli) gdcli[sid]=maxclient;
						}
					}
					if (gdsend[sid]){
            			sendgamedata(sid);
            			gdopen[sid]=false;
            			gdsend[sid]=false;
            			gdcli[sid]=-1;
            		}
            		return;
				}
				for (int i = 0; i < maxclienttr[sid]; i++) {
					if (gameclientport[sid][i]==0 || gameclientIP[sid][i]==null || pcli==i || (!gameclientrdy[sid][i] && (pname==1 || pname==15)) || (gameclientport[sid][i]==port && gameclientIP[sid][i].equals(ip))) continue;
					if (pp) {
						paknum=gamesendpaknum[sid][i];
						byte[] buf=ByteBuffer.allocate(4).putInt(paknum).array();
						data[3]=buf[0];
						data[4]=buf[1];
						data[5]=buf[2];
						data[6]=buf[3];
					}
					if (gameclientIP[sid][i]==null) continue;
					DatagramPacket pak = new DatagramPacket(data, datalength, gameclientIP[sid][i], gameclientport[sid][i]);
					gameUdpSocket[sid].send(pak);
					if (pp && paknum!=-1) addgamepp(sid, i, data, datalength, gameclientIP[sid][i], gameclientport[sid][i], paknum);
					if (pp) gamesendpaknum[sid][i]++;
				}
			}else{
				if (pnaz>=0 && pnaz<maxclienttr[sid]){
					if (gameclientport[sid][pnaz]!=0 && gameclientIP[sid][pnaz]!=null){
						if (pp) {
							paknum=gamesendpaknum[sid][pnaz];
							byte[] buf=ByteBuffer.allocate(4).putInt(paknum).array();
							data[3]=buf[0];
							data[4]=buf[1];
							data[5]=buf[2];
							data[6]=buf[3];
						}
						if (gameclientIP[sid][pnaz]==null) return;
						DatagramPacket pak = new DatagramPacket(data, datalength, gameclientIP[sid][pnaz], gameclientport[sid][pnaz]);
						gameUdpSocket[sid].send(pak);
						if (pp && paknum!=-1) addgamepp(sid, pnaz, data, datalength, gameclientIP[sid][pnaz], gameclientport[sid][pnaz], paknum);
						if (pp) gamesendpaknum[sid][pnaz]++;
					}
				}
			}
		} catch (NullPointerException | IOException e) {
			timelog("obrpaket sid="+ sid +" error: "+ e);
		}
    }
	
	static boolean addgamepr(int sid, int cid, int num, byte[] data, int length, InetAddress ip, int port){//dobavlenije prinjatogo paketa v buffer
		if (log==sid) log("addgamepr sid="+ sid +" cid="+ cid +" num="+ num +" recnum="+ gamereceivedpaknum[sid][cid] +" pname="+String.valueOf(data[0]));
		if (cid==-1 || data==null || ip==null || port==0) return false;
		for (int i = 0; i < maxpr; i++) {
			if (prgameactiv[sid][cid][i] && prgamenum[sid][cid][i]==num) return true;
		}
		byte[] pakdata = new byte[length];
		System.arraycopy(data, 0, pakdata, 0, length);
    	DatagramPacket pak = new DatagramPacket(pakdata, length, ip, port);
		for (int i = 0; i < maxpr; i++) {
			if (!prgameactiv[sid][cid][i]){
				prgameactiv[sid][cid][i]=true;
				prgamenum[sid][cid][i]=num;
				prgamepak[sid][cid][i]=pak;
				return true;
			}
		}
		int ii=-1;
		int min=0;
		for (int i = 0; i < maxpr; i++) {
			if (prgamenum[sid][cid][i]<min || ii==-1){
				ii=i;
				min=prgamenum[sid][cid][i];
			}
		}
        if (min > gamereceivedpaknum[sid][cid]) {
            gamereceivedpaknum[sid][cid] = min;
        } else {
            gamereceivedpaknum[sid][cid]++;
        }
        prgameactiv[sid][cid][ii]=true;
        prgamenum[sid][cid][ii]=num;
        prgamepak[sid][cid][ii]=pak;
        return true;
    }
	
	static void cleargamepr(int sid, int cid){//clear buffer priema
		for (int t = 0; t < maxpr; t++) {
			prgamepak[sid][cid][t]=null;
			prgamenum[sid][cid][t]=0;
			prgameactiv[sid][cid][t]=false;
		}
	}

	static void delgamepr(int sid, int cid, int irp){//del paket iz buffera priema
		if (cid==-1) return;
		prgamepak[sid][cid][irp]=null;
		prgamenum[sid][cid][irp]=0;
		prgameactiv[sid][cid][irp]=false;
	}
	
	static int findgamepr(int sid, int cid, int num){//poisk paketa v buffere priema
        if (cid==-1) return -1;
        for (int i = 0; i < maxpr; i++) {
            if (prgameactiv[sid][cid][i] && prgamenum[sid][cid][i]==num) return i;
        }
        return -1;
    }
	
	static void sendpname(int pname, InetAddress ip, int port){//otpravka otveta clientu
		if (ip==null) return;
		try{
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            DataOutputStream daos=new DataOutputStream(baos);
            daos.writeByte((byte)pname);
            daos.close();
            final byte[] bytes=baos.toByteArray();
            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, ip, port);
            mainUdpSocket.send(pak);
        }catch (NullPointerException | IOException e){
        	timelog("sendpname pname="+ pname +" error: "+ e);
        }
    }
	
	static boolean sendgamepname(int sid, int pname, InetAddress destip, int destport){//otpravka otveta clientu
		if (destip==null) return false;
		try{
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            DataOutputStream daos=new DataOutputStream(baos);
            daos.writeByte(pname);
            daos.close();
            final byte[] bytes=baos.toByteArray();
            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, destip, destport);
            if (gameUdpSocket[sid]!=null && !gameUdpSocket[sid].isClosed()) gameUdpSocket[sid].send(pak);
            return true;
        }catch (NullPointerException | IOException e){
        	timelog("sendgamepname pname="+ pname +" error: "+ e);
        }
        return false;
	}
	
	static void sendcreategame(InetAddress destip, int destport, int gameport){//otpravka komandi na sozdanie igri
		if (destip==null) return;
		try{
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            DataOutputStream daos=new DataOutputStream(baos);
            daos.writeByte(-5);//_creategame_
            daos.writeInt(gameport);
            daos.close();
            final byte[] bytes=baos.toByteArray();
            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, destip, destport);
            mainUdpSocket.send(pak);
            addmainpp(pak);
        }catch (NullPointerException | IOException e){
        	timelog("sendcreategame gameport="+ gameport +" error: "+ e);
        }
    }
	
	static void sendserverlist(InetAddress destip, int destport, int sver){//otpravka spiska sozdannich igr
		if (destip==null) return;
		try{
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            DataOutputStream daos=new DataOutputStream(baos);
            daos.writeByte(-7);//_serverlist_
            if (sver>=5) daos.writeShort(getservercount());
            short count=0;
            for (int i = 0; i < maxserver; i++) {
            	if (gameclientport[i][0]==0 || gameport[i]==0) continue;
            	if (daos.size()>plength-55) {
            		daos.close();
                    final byte[] bytes=baos.toByteArray();
                    if (sver>=5) {
                    	byte[] buf=ByteBuffer.allocate(2).putShort(count).array();
                    	bytes[1]=buf[0];
                    	bytes[2]=buf[1];
                    }
                    DatagramPacket pak = new DatagramPacket(bytes, bytes.length, destip, destport);
                    mainUdpSocket.send(pak);
                    baos=new ByteArrayOutputStream();
                    daos=new DataOutputStream(baos);
                    daos.writeByte(-7);
                    if (sver>=5) daos.writeShort(getservercount()-count);
                    count=0;
            	}
            	count++;
            	daos.writeInt(gameport[i]);
                daos.writeByte(gamenmap[i]);
                if (sver>=11) {
                	boolean custommap=gamecustommap[i];
                	daos.writeBoolean(custommap);
                	if (custommap) {
                		daos.writeUTF(gamemapname[i]);
                		daos.writeUTF(gamemapuid[i]);
                	}
                }
                daos.writeByte(gamegtype[i]);
                if (sver<4){
                	daos.writeByte(gameplcount[i]);
                }else{
                	int plcount=0;
                	for (int t = 0; t < maxclienttr[i]; t++) {
                		if (gameclientport[i][t]!=0) plcount++;
					}
                	daos.writeByte(plcount);//players
                	daos.writeByte(gameplcount[i]-plcount);//bots
                }
                daos.writeByte(gameversiongame[i]);
                daos.writeUTF(gameverstr[i]);
                daos.writeShort(gameping[i]);
                if (sver>=2) {
                	if (sver>=5){
                		daos.writeUTF(gamesrvname[i]);
                		if (sver>=7) {
                			daos.writeBoolean(gamesrvpass[i]!=null);//server password?
                			daos.writeByte(gamesrvstatus[i]);
                		}
                	}else{
                		int len=gamesrvname[i].getBytes(StandardCharsets.UTF_8).length;
                		if (len>0 && len<100){
                			daos.writeShort(len);
                			daos.write(gamesrvname[i].getBytes(StandardCharsets.UTF_8));
                		}else{
                			daos.writeShort(0);
                		}
                	}
                }
			}
            daos.close();
            final byte[] bytes=baos.toByteArray();
            DatagramPacket pak = new DatagramPacket(bytes, bytes.length, destip, destport);
            mainUdpSocket.send(pak);
        }catch (NullPointerException | IOException e){
        	timelog("sendserverlist error: "+ e);
        }
    }
	
	static void addmainpp(DatagramPacket pak){//dobavit paket s podtversdeniem
		if (pak==null) return;
		synchronized (ppmainlock){
			for (int i = 0; i < maxpp; i++) {
				if (!ppmainactiv[i]){
					try{
						ppmainactiv[i]=true;
						ppmaintimer[i]=ppperiod;
						ppmaindeltimer[i]=0;
						ppmainpak[i]=pak;
					}catch (NullPointerException e){
						ppmainactiv[i]=false;
						timelog("addmainpp error: "+ e);
					}
					return;
				}
			}
		}
		timelog("addmainpp: array is foll.");
	}

	static void obrmainpptimer(){//obrabotka paketov s podtversdeniem
		synchronized (ppmainlock){
			for (int t = 0; t < maxpp; t++) {
				if (ppmainactiv[t]) ppmaintimer[t]--;
				if (ppmainactiv[t] && ppmaintimer[t]<=0) {
					ppmaindeltimer[t]++;
					DatagramPacket pak = ppmainpak[t];
					if (pak==null){
						ppmainactiv[t]=false;
						continue;
					}
					if (ppmaindeltimer[t]>10){
						delmainpp(pak.getAddress(), pak.getPort());
						continue;
					}
					try {
						ppmaintimer[t]=ppperiod+ppmaindeltimer[t]*ppperiod/2;
						mainUdpSocket.send(pak);
					} catch (NullPointerException | IOException e){
						timelog("obrmainpptimer error: "+ e);
					}
                }
			}
		}
	}
    
    static void delmainpp(InetAddress ip, int port){//udalenie paketa so spiska podtversdenija
    	if (ip==null) return;
    	try{
    		synchronized (ppmainlock){
    			for (int i=0; i<maxpp; i++) {
    				DatagramPacket pak=ppmainpak[i];
    				if (ppmainactiv[i] && pak!=null && pak.getPort()==port && ip.equals(pak.getAddress())) {
    					ppmaindeltimer[i]=0;
    					ppmainpak[i]=null;
    					ppmainactiv[i]=false;
    					return;
    				}
    			}
    		}
    	}catch(NullPointerException e){
    		timelog("delmainpp error: "+ e);
    	}
    }
    
    static void addgamepp(int sid, int cid, byte[] data, int length, InetAddress ip, int port, int num){//dobavit paket s podtversdeniem
    	if (data==null || ip==null || length<=0 || port==0) return;
    	for (int i = 0; i < maxpp; i++) {
    		if (ppgameactiv[sid][cid][i] && ppgamenum[sid][cid][i]==num) return;
    	}
    	addsping(sid, cid, num);
    	byte[] pakdata = new byte[length];
		System.arraycopy(data, 0, pakdata, 0, length);
    	DatagramPacket pak = new DatagramPacket(pakdata, length, ip, port);
    	synchronized (ppgamelock[sid]){
    		for (int i = 0; i < maxpp; i++) {
    			if (!ppgameactiv[sid][cid][i]){
    				try{
    					ppgameactiv[sid][cid][i]=true;
    					ppgametimer[sid][cid][i]=gameclientppt[sid][cid];
    					ppgamedeltimer[sid][cid][i]=0;
    					ppgamepak[sid][cid][i]=pak;
    					ppgamenum[sid][cid][i]=num;
    				}catch (NullPointerException e){
    					ppgameactiv[sid][cid][i]=false;
    					timelog("addgamepp error: "+ e);
    				}
    				return;
    			}
    		}
    		int ii=-1;
    		int min=0;
    		for (int i = 0; i < maxpp; i++) {
    			if (ppgamenum[sid][cid][i]<min || ii==-1){
    				min=ppgamenum[sid][cid][i];
    				ii=i;
    			}
    		}
    		if (ii!=-1){
    			try{
    				ppgameactiv[sid][cid][ii]=true;
    				ppgametimer[sid][cid][ii]=gameclientppt[sid][cid];
    				ppgamedeltimer[sid][cid][ii]=0;
    				ppgamepak[sid][cid][ii]=pak;
    				ppgamenum[sid][cid][ii]=num;
    			}catch (NullPointerException e){
    				ppgameactiv[sid][cid][ii]=false;
    				timelog("addgamepp error: "+ e);
    			}
    		}
    	}
    }
	
    static void obrgamepptimer() throws IOException {//obrabotka paketov s podtversdeniem
    	for (int i = 0; i < maxserver; i++) {
    		if (gameclientport[i][0]==0) continue;
    		synchronized (ppgamelock[i]){
    			for (int c = 0; c < maxclienttr[i]; c++) {
    				if (gameclientport[i][c]==0) continue;
    				for (int t = 0; t < maxpp; t++) {
    					if (ppgameactiv[i][c][t]) ppgametimer[i][c][t]--;
    					if (ppgameactiv[i][c][t] && ppgametimer[i][c][t]<=0) {
    						DatagramPacket pak = ppgamepak[i][c][t];
    						if (pak==null){
    							delgamepp(i, c, t);
    							continue;
    						}
    						ppgamedeltimer[i][c][t]++;
    						if (ppgamedeltimer[i][c][t]>4){
    							delgamepp(i, c, t);
    							continue;
    						}
    						try {
    							int ppt=gameclientppt[i][c];
    							ppgametimer[i][c][t]=ppt+ppt*ppgamedeltimer[i][c][t]/2;
    							int clport=gameclientport[i][c];
    							if (pak.getPort()!=clport && clport!=0) pak.setPort(clport);
    							if (i==log) timelog("povt send num="+ ppgamenum[i][c][t] +" cid="+ c +" pname="+String.valueOf(pak.getData()[0]));
    							gameUdpSocket[i].send(pak);
    						} catch (NullPointerException e) {
    							timelog("obrgamepptimer sid="+ i +" cid="+ c +" error: "+ e);
    						}
						}
    				}
    			}
    		}
    	}
    }
    
    static void delgamepp(int sid, int cid, int num){//udalenie paketa so spiska podtversdenija
    	synchronized (ppgamelock[sid]){
    		for (int i=0; i<maxpp; i++) {
    			if (ppgameactiv[sid][cid][i] && ppgamenum[sid][cid][i]==num) {
    				ppgamepak[sid][cid][i]=null;
    				ppgamedeltimer[sid][cid][i]=0;
    				ppgameactiv[sid][cid][i]=false;
    				return;
    			}
    		}
    	}
    }
    
    static void cleargamepp(int sid, int cid){//clear massiv paketov s podtversdeniem
    	synchronized (ppgamelock[sid]){
    		for (int i=0; i<maxpp; i++) {
    			ppgamepak[sid][cid][i]=null;
    			ppgamedeltimer[sid][cid][i]=0;
    			ppgameactiv[sid][cid][i]=false;
    		}
    	}
    }
    
    static void addsping(int sid, int cid, int num){//add send ping time
    	pingpaktr[sid][cid][0]=0;
    	pingpaknum[sid][cid][0]=num;
    }

    static void addrping(int sid, int cid, int num){//add rec ping time
        for (int i=0; i<maxpingpak; i++) {
            if (pingpaknum[sid][cid][i]==num){
                pingpaktr[sid][cid][i]=1;
                return;
            }
        }
    }

    static int getppt(int sid, int cid){//vremja pereotpravki paketa
        for (int i=0; i<maxpingpak; i++) {
            if (pingpaktr[sid][cid][i]!=0) return i;
        }
        return -1;
    }

    static void obrping(){//set onlinepptime
    	for (int i = 0; i < maxserver; i++) {
    		if (gameclientport[i][0]==0) continue;
    		for (int c = 0; c < maxclienttr[i]; c++) {
    			if (gameclientport[i][c]==0) continue;
    			int ppt=getppt(i,c);
    			if (ppt!=-1){
    				ppt=ppt+5;
    				if (ppt<5) ppt=5;
    				if (ppt>30) ppt=30;
    				gameclientppt[i][c]=ppt;
    			}
    			for (int t=maxpingpak-1; t>0; t--) {
                    pingpaknum[i][c][t]=pingpaknum[i][c][t-1];
                    pingpaktr[i][c][t]=pingpaktr[i][c][t-1];
                }
    			pingpaktr[i][c][0]=0;
                pingpaknum[i][c][0]=0;
    		}
    	}
    }
    
    static void clearping(int sid, int cid){
    	for (int i=0; i<maxpingpak; i++) {
    		pingpaknum[sid][cid][i]=0;
    		pingpaktr[sid][cid][i]=0;
    	}
    }
}