import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.nio.file.Files;


public class dServer {
    public static void main(String[] args) throws IOException{
        int port  = Integer.parseInt("8002");
        String localStoragePath = "C:\\Users\\asus\\Desktop\\DataStorage";

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
        System.out.println("Data nodes store data in " + localStoragePath);
        new DataServer(port, localStoragePath).start();

//        if (InetAddress.getLocalHost().getHostName().equals("master")){
//            new NameServer(port, localStoragePath).service();
//        }
//        else{
//            new DataServer(port, localStoragePath).service();
//        }
    }
}


class DataServer{
    private ServerSocket serverSocket;
    private String localStoragePath;
    private final static String fSeparator = System.getProperty("file.separator");

    DataServer(int port, String localStoragePath) throws IOException{
        this.localStoragePath = localStoragePath;
        serverSocket=new ServerSocket(port);
    }

    private String echo(String msg){
        return "echo:"+msg;
    }

    private String saveFile(String msg, Socket socket){
        String[] arr = msg.split("\\s+");
        String targetLocalPath = localStoragePath + arr[1];
        File dir = new File(targetLocalPath.substring(0, targetLocalPath.lastIndexOf(fSeparator)));
        if (!dir.exists()){
            if (!dir.mkdirs()) {
                return "Fail";
            }
        }

        String retMsg = "Fail";
        try {
            if (new File(targetLocalPath).createNewFile()){
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                FileOutputStream fw = new FileOutputStream(targetLocalPath);


                byte[] bytes = new byte[1024];
                int num = 0;
                while((num = in.read(bytes, 0, bytes.length)) != -1) {
                    fw.write(bytes, 0, num);
                    fw.flush();
                }

                // Codes below cause error, even after close this program,
                // we would find the notepad and notepad++ crush when opening the file
                // And the file is always of blockSize

//                byte[] b = new byte[blockSize];
//                while(in.read(b)!=-1){
//                    fw.write(b);
//                }
                retMsg = "OK";
                fw.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                socket.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return retMsg;
    }

    private String deleteFile(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length!=2) return "Parameters error";
        String targetLocalPath = localStoragePath + arr[1];
        File f = new File(targetLocalPath);
        File p = f.getParentFile();
        boolean status = true;
        if (p.exists() && p.isDirectory()) {
            File[] files = p.listFiles();
            if (files!=null) {
                for (File file : files) {
                    String filePath = file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf('-'));
                    if (filePath.equals(targetLocalPath)){
                        if (!file.delete()){
                            status = false;
                        }
                    }
                }
            }
        }
        if (status) {
            return "OK";
        }
        else {
            return "Error";
        }
    }

    private String deleteDir(String msg){
        String[] arr = msg.split("\\s+");
        if (arr.length != 2) return "Parameters error...";
        File root = new File(localStoragePath + arr[1]);
        if (root.exists() && root.isDirectory()) {
            return recDeleteDir(root);
        }
        else {
            return "Not a directory...";
        }
    }

    private String recDeleteDir(File root){
        File[] files = root.listFiles();
        boolean status = true;
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    recDeleteDir(f);
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
        if(!root.delete()){
            status=false;
        }
        if(status) {
            return "OK";
        }
        else {
            return "Error deleting file...";
        }
    }

    private String getFile(String msg, Socket socket){
        String[] arr = msg.split("\\s+");
        if (arr.length != 2) return "Parameters error...";
        File f = new File(localStoragePath + arr[1]);
        if (!f.exists() || f.isDirectory()){
            return "File not exist or is directory.";
        }
        try {
            FileInputStream fi = new FileInputStream(f);
            OutputStream netWriter = socket.getOutputStream();
            netWriter.write(fi.readAllBytes());
            fi.close();
            netWriter.close();

            socket.close();
            return "OK";
        } catch (IOException e) {
            e.printStackTrace();

            try {
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return "Error";
        }
    }

    void start(){
        System.out.println("Data node on!");
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                System.out.println("New connection accepted" + socket.getInetAddress() + ":" + socket.getPort());

                BufferedReader br = getReader(socket);
                PrintWriter pw = getWriter(socket);

                String msg = null;
                if ((msg = br.readLine()) != null) {
                    System.out.println(msg);
                    String cmd =  msg.split(" ")[0];
                    switch (cmd) {
                        case "echo":
                            pw.println(echo("command: " + cmd));
                            pw.println(echo("message: " + msg));
                            break;
                        case "hi":
                            pw.println("hi");
                            break;
                        case "save":
                            pw.println("OK");
                            pw.println(saveFile(msg, socket));
                            break;
                        case "del-file":
                            deleteFile(msg);
                            break;
                        case "del-dir":
                            deleteDir(msg);
                            break;
                        case "get":
                            getFile(msg, socket);
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


