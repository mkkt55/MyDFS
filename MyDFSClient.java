import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class MyDFSClient {

    public static void main(String[] args) throws IOException {
        new Client().talk();
    }
}

class Client{
    private final static String fSeparator = System.getProperty("file.separator");

    private String host="localhost";
    private int port=8001;
    private Socket socket;
    private String curDFSPath = fSeparator;

    Client() throws IOException {
        socket=new Socket(host, port);
    }

    private PrintWriter getWriter(Socket socket)throws IOException{
        OutputStream socketOut=socket.getOutputStream();
        return new PrintWriter(socketOut,true);
    }

    private BufferedReader getReader(Socket socket) throws IOException{
        InputStream socketIn=socket.getInputStream();
        return new BufferedReader(new InputStreamReader(socketIn));
    }

    void talk() throws IOException{
        try {
            BufferedReader br=getReader(socket);
            PrintWriter pw=getWriter(socket);
            BufferedReader localReader=new BufferedReader(new InputStreamReader(System.in));
            String msg = null;
            System.out.print("MyDFS " + fSeparator + "> ");
            while((msg=localReader.readLine())!=null){
                String[] arr = msg.split("\\s+");
                String cmd =  arr[0];


                if(msg.equals("exit")){
                    System.out.println("Bye");
                    socket.close();
                    break;
                }

                StringBuilder formatMsg = new StringBuilder(cmd);
                for (int i = 1; i < arr.length; i++){
                    formatMsg.append(" ").append(arr[i]);
                }
                pw.println(formatMsg);

                String retMsg = br.readLine();
                String[] retArr = retMsg.split("\\s+");
                if (retArr[0].equals("cd:")){
                    curDFSPath = retArr[1];
                }
//                System.out.println("We received: " + retMsg);
                System.out.println(retMsg);
                System.out.print("MyDFS " + curDFSPath +"> ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}



