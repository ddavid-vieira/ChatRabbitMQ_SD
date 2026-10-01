package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class Chat {

  private static final String HOST = env("RABBIT_HOST", "54.237.198.85");
  private static final String USER = env("RABBIT_USER", "admin");
  private static final String PASS = env("RABBIT_PASS", "password");
  

  private static volatile String prompt = "<< ";

  public static void main(String[] argv) throws Exception {
    
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(HOST);
    factory.setUsername(USER);
    factory.setPassword(PASS);
    factory.setVirtualHost("/");

    Scanner scanner = new Scanner(System.in, "UTF-8");

    String usuario = "";
    while (usuario.isEmpty()) {
      System.out.print("User: ");
      if (!scanner.hasNextLine()) return;
      usuario = scanner.nextLine().trim();
    }

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(usuario,    true,   false,     false,       null);

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String mensagem = new String(body, StandardCharsets.UTF_8);
        synchronized (System.out) {
          System.out.println();
          System.out.println(mensagem);
          System.out.print(prompt);
          System.out.flush();
        }
      }
    };

    channel.basicConsume(usuario,    true,    consumer);

    String destinatario = null;
    SimpleDateFormat formato = new SimpleDateFormat("dd/MM/yyyy 'às' HH:mm");

    while (true) {
      synchronized (System.out) {
        System.out.print(prompt);
        System.out.flush();
      }
      if (!scanner.hasNextLine()) break;
      String linha = scanner.nextLine();
      String texto = linha.trim();

      if (texto.isEmpty()) continue;

      if (texto.startsWith("@")) {
        String novo = texto.substring(1).trim();
        if (novo.isEmpty() || novo.contains(" ")) {
          System.out.println("Nome de usuário inválido.");
          continue;
        }
        destinatario = novo;
        prompt = "@" + destinatario + "<< ";
        continue;
      }

      if (destinatario == null) {
        System.out.println("Escolha um destinatário com @nome antes de enviar mensagens.");
        continue;
      }

      channel.queueDeclare(destinatario, true, false, false, null);

      String mensagem = "(" + formato.format(new Date()) + ") @" + usuario + " diz: " + linha;

      channel.basicPublish("",       destinatario, null,  mensagem.getBytes(StandardCharsets.UTF_8));
    }

    channel.close();
    connection.close();
  }

  private static String env(String nome, String padrao) {
    String v = System.getenv(nome);
    return (v == null || v.isEmpty()) ? padrao : v;
  }
}