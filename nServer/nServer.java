import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;


public class nServer {
    public static void main(String[] args) throws IOException{
        int port  = Integer.parseInt("8001");
        String localStoragePath = "C:\\Users\\asus\\Desktop\\NameStorage";

        File dir = new File(localStoragePath);

        if (!dir.exists()){
            if (dir.mkdirs()){
                System.out.println("Create local storage path: " + localStoragePath);
            }
            else {
                System.out.println("Fail to access or create local storage path: " + localStoragePath);
                return;
            }
        }
        System.out.println("Name node stores data in " + localStoragePath);
        new NameServer(port, localStoragePath).start();
//        new DataServer(8002, DataStoragePath).service();



//        if (InetAddress.getLocalHost().getHostName().equals("master")){
//            new NameServer(port, localStoragePath).service();
//        }
//        else{
//            new DataServer(port, localStoragePath).service();
//        }
    }
}

class NameServer{
    private final static int blockSize = 64 * 1024 *1024;
    private final static int backupNum = 2;
    private static final String fSeparator = System.getProperty("file.separator");

    private ServerSocket serverSocket;
    private String localStoragePath;

    private String curDFSPath = fSeparator;
    private List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();

    class DataNodeInfo {
        DataNodeInfo(String hostname, int port){
            this.hostname = hostname;
            this.port = port;
        }
        public String hostname;
        public int port;
        public Socket socket;
    }

    NameServer(int port, String localStoragePath) throws IOException{
        BufferedReader br = new BufferedReader(new FileReader("c:\\users\\asus\\desktop\\datanode"));
        String str;
        while((str = br.readLine()) != null){
            String[] arr = str.split("\\s+");
            dataNodeInfoList.add(new DataNodeInfo(arr[0], Integer.parseInt(arr[1])));
        }
        this.localStoragePath = localStoragePath;
        serverSocket=new ServerSocket(port);
    }

    private String getAbsoluteDFSPath(String dfsPath){
        if (dfsPath.indexOf('\\') != 0){
            // if curPath is root, path for dfsPath should be assigned specially.
            if (curDFSPath.equals("\\")) {
                dfsPath = "\\" + dfsPath;
            }
            else {
                dfsPath = curDFSPath + "\\" + dfsPath;
            }
        }
        return dfsPath;
    }

    private String echo(String msg){
        return "echo: " + msg;
    }

    private String mkdir(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length != 2){
            return "Incorrect parameters found...";
        }
        // if arr[1] is relative path
        arr[1] = getAbsoluteDFSPath(arr[1]);

        File dir = new File(localStoragePath + arr[1]);
        if (dir.mkdirs()){
            return "mkdir: " + arr[1];
        }
        return "Unable to create directory: " + arr[1];
    }

    private String upload(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length != 3){
            return "Incorrect parameters found...";
        }
        // arr[1] must be absolute path.
        // arr[2] can be either relative or absolute path.
        arr[2] = getAbsoluteDFSPath(arr[2]);

        File dir = new File(localStoragePath + arr[2].substring(0, arr[2].lastIndexOf("\\")));
        if (!dir.exists()){
            if (!dir.mkdirs()) {
                return "Fail create directory(s) for file...";
            }
        }
        // original file
        File inFile = new File(arr[1]);
        // create an empty file at name node, stores the data nodes that really stores the file.
        File localFile = new File(localStoragePath + arr[2]);
        try {
            if (localFile.createNewFile()){
                if (sendToDataNode(inFile, localFile, arr[2])) {
                    return "upload: " + arr[2];
                }
                else{
                    return "Create directory(s) for file, but fail to save file...";
                }
            }
            else{
                return "Create directory(s) for file, but fail to create file itself...";
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "Create directory(s) for file, but fail to create file itself...";
        }
    }

    private boolean sendToDataNode(File originalFile, File nameNodeFile, String dfsPath) throws IOException {
        int partNum = (int)Math.ceil(originalFile.length()/(double) blockSize);

        // for every part of the original file.
        BufferedInputStream brLocal = new BufferedInputStream(new FileInputStream(originalFile.getAbsolutePath()));

        FileWriter fwNameNode = new FileWriter(nameNodeFile);


        for (int iFile = 0; iFile < partNum; iFile++) {

            String tarFileName = dfsPath + "-" + iFile;
            fwNameNode.write(tarFileName + '\n');
            // get available data nodes
            List<DataNodeInfo> sDataNode = findDataNodes();

            // get this part of the file.
            byte[] filePart = brLocal.readNBytes(blockSize);

            // send this part of the file to all sockets.
            for (int iSocket = 0; iSocket < sDataNode.size(); iSocket++) {
                DataNodeInfo d = dataNodeInfoList.get(iSocket);
                Socket socket = d.socket;
                BufferedReader br = getReader(socket);
                PrintWriter pw = getWriter(socket);

                OutputStream netWriter = socket.getOutputStream();

                pw.println("save " + tarFileName);

                if (br.readLine().equals("OK")) {
                    // read files and send.
                    netWriter.write(filePart);
                    fwNameNode.write(d.hostname + ' ');
                }

                br.close();
                pw.close();
                netWriter.close();
                socket.close();
            }

            fwNameNode.write('\n');
        }

        brLocal.close();
        fwNameNode.close();
        return true;
    }

    // select one socket randomly, then get this one with following available sockets
    // until enough or not enough sockets available.
    private List<DataNodeInfo> findDataNodes(){
        int socketNum = backupNum;
        if ( socketNum > dataNodeInfoList.size()){
            socketNum = dataNodeInfoList.size();
        }

        List<DataNodeInfo> res = new ArrayList<>();
        // find the first one.
        int s = (int)(Math.random() * (dataNodeInfoList.size()));
        try {
            DataNodeInfo d = dataNodeInfoList.get(s);
            d.socket = new Socket(d.hostname, d.port);
            res.add(d);
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        int i = s + 1;
        if (i >= dataNodeInfoList.size()){
            i = i % dataNodeInfoList.size();
        }

        while(res.size() != socketNum && i != s) {
            try {
                DataNodeInfo d = dataNodeInfoList.get(i);
                d.socket = new Socket(d.hostname, d.port);
                res.add(d);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                i++;
                if (i >= dataNodeInfoList.size()){
                    i = i % dataNodeInfoList.size();
                }
            }
        }
        return res;
    }

    private String download(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length != 3){
            return "Incorrect parameters found...";
        }
        String dfsPath = getAbsoluteDFSPath(arr[1]);
        File newLocalFile = new File(arr[2]);
        File nameNodeLocalFile = new File(localStoragePath + dfsPath);

        try {
            newLocalFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return "Download target not available: " + newLocalFile.getAbsolutePath();
        }
        if (!newLocalFile.exists() || !nameNodeLocalFile.exists() || !newLocalFile.exists()){
            return "At least one file does not exist or is a directory, fail to execute";
        }

        try {
            BufferedReader nameNodeLocalFileReader = new BufferedReader(new FileReader(nameNodeLocalFile));
            FileOutputStream newLocalFileOutput = new FileOutputStream(newLocalFile);
            String fPartPath;

            while((fPartPath = nameNodeLocalFileReader.readLine()) != null){
                // if it is part num like \test-1, ask the file
                if (fPartPath.indexOf(fSeparator) == 0){
                    String[] hostnameArr = nameNodeLocalFileReader.readLine().split("\\s+");
                    // now find a data node in record line.
                    for (String hostname : hostnameArr){
                        // for each hostname after a file part, ask for the file,
                        // if succeed, ask the next file part.
                        boolean ifGot = false;
                        for (DataNodeInfo d : dataNodeInfoList){
                            if (d.hostname.equals(hostname)){
                                try {
                                    d.socket = new Socket(d.hostname, d.port);
                                    PrintWriter pw = getWriter(d.socket);
                                    BufferedInputStream bis = new BufferedInputStream(d.socket.getInputStream());

                                    pw.println("get " + fPartPath);

                                    byte[] bytes = new byte[1024];
                                    int num = 0;
                                    while((num = bis.read(bytes, 0, bytes.length)) != -1){
                                        newLocalFileOutput.write(bytes, 0, num);
                                        newLocalFileOutput.flush();
                                    }

                                    bis.close();
                                    pw.close();
                                    d.socket.close();
                                    ifGot = true;
                                }catch (SocketException e){
                                    e.printStackTrace();
                                }
                            }
                            break;
                        }
                        // got file in this hostname?
                        if (ifGot){
                            break;
                        }
                    }
                }
            }
            nameNodeLocalFileReader.close();
            newLocalFileOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "download";
    }

    private String listDir(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length != 1){
            return "Incorrect parameters found...";
        }

        File dir = new File(localStoragePath + curDFSPath);
        File[] files = dir.listFiles();
        StringBuilder outMsg = new StringBuilder("list: ");
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    outMsg.append(file.getName()).append("(directory) ");
                }
                else{
                    outMsg.append(file.getName()).append("(file) ");
                }
            }
        }
        return outMsg.toString();
    }

    private String cdDir(String msg){
        String newPath;
        String[] arr = msg.split("\\s+");
        if (arr.length != 2){
            return "Incorrect parameters found...";
        }

        if (arr[1].equals("..") || arr[1].equals("../")){
            if (curDFSPath.length() > 1){
                curDFSPath = curDFSPath.substring(0, curDFSPath.lastIndexOf('\\'));
                if (curDFSPath.equals("")){
                    curDFSPath = "\\";
                }
            }
            return "cd: " + curDFSPath;
        }


        newPath = getAbsoluteDFSPath(arr[1]);


        File targetDir = new File(localStoragePath + newPath);
        if (targetDir.exists()){
            if (targetDir.isDirectory()) {
                curDFSPath = newPath;
                return "cd: " + curDFSPath;
            }
            else{
                return "Fail execute \"cd " + newPath + "\", target is not directory.";
            }
        }
        else {
            return "Fail execute \"cd " + newPath + "\", directory not exist.";
        }

    }

    private String delFile(String msg){
        String[] arr = msg.split("\\s+");
        String dfsPath;
        File nameNodeFile;

        switch (arr.length){
            case 2:
                dfsPath = getAbsoluteDFSPath(arr[1]);
                nameNodeFile = new File(localStoragePath + dfsPath);
                if (!nameNodeFile.exists() || nameNodeFile.isDirectory()){
                    return "Target not exists or is a directory...";
                }
                break;
            case 3:
                if (!arr[1].equals("-r")) {
                    return "Incorrect parameters found...";
                }
                dfsPath = getAbsoluteDFSPath(arr[2]);
                nameNodeFile = new File(localStoragePath + dfsPath);
                if (!nameNodeFile.exists() || !nameNodeFile.isDirectory()){
                    return "Target not exists, or is not a directory...";
                }
                break;
            default:
                return "Incorrect parameters found...";
        }

        // command is ok, and execute.
        if (nameNodeFile.isDirectory()){
            msg = deleteDFSDir(dfsPath);
        }
        else {
            msg = deleteDFSFile(dfsPath);
        }
        return msg;
    }

    private String deleteDFSFile(String dfsPath) {
        File nameNodeFile = new File(localStoragePath + dfsPath);

        boolean status = true;
        if (nameNodeFile.delete()){
            for (DataNodeInfo d : dataNodeInfoList){
                try {
                    d.socket = new Socket(d.hostname, d.port);
                    PrintWriter pw = getWriter(d.socket);
                    pw.println("del-file " + dfsPath);
                    pw.close();
                    d.socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else{
            status = false;
        }

        if (status){
            return "Delete: " + dfsPath;
        }
        else {
            return "Error deleting file";
        }
    }

    private String deleteDFSDir(String dfsPath){
        File root = new File(localStoragePath + dfsPath);
        File[] files = root.listFiles();

        boolean status = true;
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) { // 判断是否为文件夹
                    deleteDFSDir(dfsPath);
                    try {
                        if (!f.delete()){
                            status = false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        if (!f.delete()){
                            status = false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        if (!root.delete()){
            status = false;
        }
        for(DataNodeInfo d : dataNodeInfoList){
            try {
                d.socket = new Socket(d.hostname, d.port);
                PrintWriter pw = getWriter(d.socket);
                pw.println("del-dir " + dfsPath);
                pw.close();
                d.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (status) {
            return "Delete directory and all files in it: " + dfsPath;
        }
        else {
            return "Error deleting directory.";
        }
    }


    void start(){
        System.out.println("Name node on!");
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                System.out.println("New connection accepted" + socket.getInetAddress() + ":" + socket.getPort());

                BufferedReader br = getReader(socket);
                PrintWriter pw = getWriter(socket);

                String msg = null;
                while ((msg = br.readLine()) != null) {
//                    System.out.println("msg: " + msg);
                    String cmd =  msg.split("\\s+")[0];
//                    System.out.println("cmd: " + cmd);

                    switch (cmd) {
                        case "echo":
                            pw.println(echo("command: " + cmd));
                            pw.println(echo("message: " + msg));
                            break;
                        case "mkdir":
                            pw.println(mkdir(msg));
                            break;
                        case "upload":
                            pw.println(upload(msg));
                            break;
                        case "download":
                            pw.println(download(msg));
                            break;
                        case "ls":
                            pw.println(listDir(msg));
                            break;
                        case "cd":
                            pw.println(cdDir(msg));
                            break;
                        case "rm":
                            pw.println(delFile(msg));
                            break;
                        default:
                            pw.println("Sorry, unknown command: " + cmd);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private PrintWriter getWriter(Socket socket) throws IOException{
        OutputStream socketOut=socket.getOutputStream();
        return new PrintWriter(socketOut,true);
    }

    private BufferedReader getReader(Socket socket) throws IOException{
        InputStream socketIn=socket.getInputStream();
        return new BufferedReader(new InputStreamReader(socketIn));
    }
}

