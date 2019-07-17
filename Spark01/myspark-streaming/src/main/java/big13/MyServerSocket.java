package big13;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class MyServerSocket {
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket ss = new ServerSocket(8888);
        while (true) {
            Socket socket = ss.accept();
            System.out.println("someone is accepting....");
            OutputStream oos = socket.getOutputStream();
            int i = 0;
            for (; ; ) {
                oos.write(("hello world tom" + i + "\r\n").getBytes());
                oos.flush();
                Thread.sleep(100);
                i++;
            }
        }

    }
}
