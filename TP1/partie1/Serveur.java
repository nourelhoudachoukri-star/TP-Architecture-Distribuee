   import java.io.*;
   import java.net.*;

   public class Serveur {
       public static void main(String[] args) throws IOException {
           ServerSocket serverSocket = new ServerSocket(5000);
           System.out.println("Serveur lancé sur le port 5000...");
           Socket clientSocket = serverSocket.accept();

           PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
           BufferedReader in = new BufferedReader(
                   new InputStreamReader(clientSocket.getInputStream()));

           String message = in.readLine();
           System.out.println("Message reçu : " + message);
           out.println("Hello Client, votre message a été reçu !");

           clientSocket.close();
           serverSocket.close();
       }
   }