package br.ufs.dcomp.ChatRabbitMQ;

import br.ufs.dcomp.ChatRabbitMQ.MensagemProto.Conteudo;
import br.ufs.dcomp.ChatRabbitMQ.MensagemProto.Mensagem;
import com.google.protobuf.ByteString;
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
    final String eu = usuario;

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(usuario,    true,   false,     false,       null);

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        Mensagem mensagem = Mensagem.parseFrom(body);
        String grupo = mensagem.getGrupo();

        // O exchange do grupo replica a mensagem também para a fila do emissor
        if (!grupo.isEmpty() && mensagem.getEmissor().equals(eu)) return;

        String origem = grupo.isEmpty()
            ? "@" + mensagem.getEmissor()
            : mensagem.getEmissor() + "#" + grupo;
        String texto = mensagem.getConteudo().getCorpo().toString(StandardCharsets.UTF_8);

        synchronized (System.out) {
          System.out.println();
          System.out.println("(" + mensagem.getData() + " às " + mensagem.getHora() + ") "
              + origem + " diz: " + texto);
          System.out.print(prompt);
          System.out.flush();
        }
      }
    };

    channel.basicConsume(usuario,    true,    consumer);

    // Destino atual: "@nome" para usuário ou "#nome" para grupo
    String destino = null;
    SimpleDateFormat formatoData = new SimpleDateFormat("dd/MM/yyyy");
    SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm");

    while (true) {
      synchronized (System.out) {
        System.out.print(prompt);
        System.out.flush();
      }
      if (!scanner.hasNextLine()) break;
      String linha = scanner.nextLine();
      String texto = linha.trim();

      if (texto.isEmpty()) continue;

      if (texto.startsWith("@") || texto.startsWith("#")) {
        String novo = texto.substring(1).trim();
        if (novo.isEmpty() || novo.contains(" ")) {
          System.out.println(texto.startsWith("@") ? "Nome de usuário inválido." : "Nome de grupo inválido.");
          continue;
        }
        destino = texto.charAt(0) + novo;
        prompt = destino + "<< ";
        continue;
      }

      if (texto.startsWith("!")) {
        try {
          executarComando(channel, usuario, texto.substring(1).trim());
        } catch (IOException e) {
          // Erros de canal (ex.: exchange inexistente) fecham o canal; reabre para continuar
          System.out.println("Erro ao executar comando: " + e.getMessage());
          if (!channel.isOpen()) {
            channel = connection.createChannel();
            channel.basicConsume(usuario, true, consumer);
          }
        }
        continue;
      }

      if (destino == null) {
        System.out.println("Escolha um destinatário com @usuario ou #grupo antes de enviar mensagens.");
        continue;
      }

      Date agora = new Date();
      boolean paraGrupo = destino.startsWith("#");
      String nomeDestino = destino.substring(1);

      Mensagem mensagem = Mensagem.newBuilder()
          .setEmissor(usuario)
          .setData(formatoData.format(agora))
          .setHora(formatoHora.format(agora))
          .setGrupo(paraGrupo ? nomeDestino : "")
          .setConteudo(Conteudo.newBuilder()
              .setTipo("text/plain")
              .setCorpo(ByteString.copyFrom(linha, StandardCharsets.UTF_8)))
          .build();

      try {
        if (paraGrupo) {
          channel.exchangeDeclarePassive(nomeDestino);
          channel.basicPublish(nomeDestino, "", null, mensagem.toByteArray());
        } else {
          channel.queueDeclare(nomeDestino, true, false, false, null);
          channel.basicPublish("", nomeDestino, null, mensagem.toByteArray());
        }
      } catch (IOException e) {
        System.out.println("Erro ao enviar mensagem: " + e.getMessage());
        if (!channel.isOpen()) {
          channel = connection.createChannel();
          channel.basicConsume(usuario, true, consumer);
        }
      }
    }

    channel.close();
    connection.close();
  }

  private static void executarComando(Channel channel, String usuario, String comando) throws IOException {
    String[] partes = comando.split("\\s+");

    switch (partes[0]) {
      case "addGroup":
        if (partes.length != 2) {
          System.out.println("Uso: !addGroup <grupo>");
          return;
        }
        channel.exchangeDeclare(partes[1], BuiltinExchangeType.FANOUT, true);
        channel.queueBind(usuario, partes[1], "");
        break;

      case "addUser":
        if (partes.length != 3) {
          System.out.println("Uso: !addUser <usuario> <grupo>");
          return;
        }
        channel.exchangeDeclarePassive(partes[2]);
        channel.queueDeclare(partes[1], true, false, false, null);
        channel.queueBind(partes[1], partes[2], "");
        break;

      case "removeUser":
        if (partes.length != 3) {
          System.out.println("Uso: !removeUser <usuario> <grupo>");
          return;
        }
        channel.queueUnbind(partes[1], partes[2], "");
        break;

      case "removeGroup":
        if (partes.length != 2) {
          System.out.println("Uso: !removeGroup <grupo>");
          return;
        }
        channel.exchangeDelete(partes[1]);
        break;

      default:
        System.out.println("Comando desconhecido: !" + partes[0]);
    }
  }

  private static String env(String nome, String padrao) {
    String v = System.getenv(nome);
    return (v == null || v.isEmpty()) ? padrao : v;
  }
}
