package br.ufs.dcomp.ChatRabbitMQ;

import br.ufs.dcomp.ChatRabbitMQ.MensagemProto.Conteudo;
import br.ufs.dcomp.ChatRabbitMQ.MensagemProto.Mensagem;
import com.google.protobuf.ByteString;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Chat {

  private static final String HOST = env("RABBIT_HOST", "54.234.8.254");
  private static final String USER = env("RABBIT_USER", "admin");
  private static final String PASS = env("RABBIT_PASS", "password");

  private static final String SUFIXO_ARQUIVOS = "_arquivos";

  private static final long TAMANHO_MAXIMO = 64L * 1024 * 1024;

  private static final DateTimeFormatter FORMATO_DATA = DateTimeFormatter.ofPattern("dd/MM/yyyy");
  private static final DateTimeFormatter FORMATO_HORA = DateTimeFormatter.ofPattern("HH:mm");

  private static final Path PASTA_DOWNLOADS = pastaDownloads();

  private static volatile String prompt = "<< ";

  private static Connection connection;
  private static ExecutorService transferencias;

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

    Files.createDirectories(PASTA_DOWNLOADS);
    System.out.println("Arquivos recebidos serão salvos em " + PASTA_DOWNLOADS);

    transferencias = Executors.newCachedThreadPool(tarefa -> {
      Thread t = new Thread(tarefa, "transferencia");
      t.setDaemon(true);
      return t;
    });

    connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(usuario,                    true, false, false, null);
    channel.queueDeclare(usuario + SUFIXO_ARQUIVOS,  true, false, false, null);

    Consumer consumidorTexto = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
        AMQP.BasicProperties properties, byte[] body) throws IOException {
        Mensagem mensagem = Mensagem.parseFrom(body);

        if (!mensagem.getGrupo().isEmpty() && mensagem.getEmissor().equals(eu)) return;

        String texto = mensagem.getConteudo().getCorpo().toString(StandardCharsets.UTF_8);
        exibir("(" + mensagem.getData() + " às " + mensagem.getHora() + ") "
            + origem(mensagem) + " diz: " + texto);
      }
    };

    Consumer consumidorArquivos = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
        AMQP.BasicProperties properties, byte[] body) throws IOException {
        Mensagem mensagem = Mensagem.parseFrom(body);

        if (!mensagem.getGrupo().isEmpty() && mensagem.getEmissor().equals(eu)) return;

        transferencias.submit(() -> receberArquivo(mensagem));
      }
    };

    consumir(channel, usuario, consumidorTexto, consumidorArquivos);

    String destino = null;

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
          executarComando(channel, usuario, destino, texto.substring(1).trim());
        } catch (IOException e) {
          System.out.println("Erro ao executar comando: " + e.getMessage());
          if (!channel.isOpen()) {
            channel = connection.createChannel();
            consumir(channel, usuario, consumidorTexto, consumidorArquivos);
          }
        }
        continue;
      }

      if (destino == null) {
        System.out.println("Escolha um destinatário com @usuario ou #grupo antes de enviar mensagens.");
        continue;
      }

      boolean paraGrupo = destino.startsWith("#");
      String nomeDestino = destino.substring(1);

      Mensagem mensagem = novaMensagem(usuario, paraGrupo ? nomeDestino : "")
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
          consumir(channel, usuario, consumidorTexto, consumidorArquivos);
        }
      }
    }

    transferencias.shutdown();
    transferencias.awaitTermination(30, TimeUnit.SECONDS);

    channel.close();
    connection.close();
  }

  private static void consumir(Channel channel, String usuario, Consumer texto, Consumer arquivos)
      throws IOException {
    channel.basicConsume(usuario,                   true, texto);
    channel.basicConsume(usuario + SUFIXO_ARQUIVOS, true, arquivos);
  }

  private static void executarComando(Channel channel, String usuario, String destino, String comando)
      throws IOException {
    String[] partes = comando.split("\\s+");

    switch (partes[0]) {
      case "addGroup":
        if (partes.length != 2) {
          System.out.println("Uso: !addGroup <grupo>");
          return;
        }
        channel.exchangeDeclare(partes[1], BuiltinExchangeType.FANOUT, true);
        channel.exchangeDeclare(partes[1] + SUFIXO_ARQUIVOS, BuiltinExchangeType.FANOUT, true);
        channel.queueBind(usuario, partes[1], "");
        channel.queueBind(usuario + SUFIXO_ARQUIVOS, partes[1] + SUFIXO_ARQUIVOS, "");
        break;

      case "addUser":
        if (partes.length != 3) {
          System.out.println("Uso: !addUser <usuario> <grupo>");
          return;
        }
        channel.exchangeDeclarePassive(partes[2]);
        channel.exchangeDeclare(partes[2] + SUFIXO_ARQUIVOS, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(partes[1], true, false, false, null);
        channel.queueDeclare(partes[1] + SUFIXO_ARQUIVOS, true, false, false, null);
        channel.queueBind(partes[1], partes[2], "");
        channel.queueBind(partes[1] + SUFIXO_ARQUIVOS, partes[2] + SUFIXO_ARQUIVOS, "");
        break;

      case "removeUser":
        if (partes.length != 3) {
          System.out.println("Uso: !removeUser <usuario> <grupo>");
          return;
        }
        channel.queueUnbind(partes[1], partes[2], "");
        channel.queueUnbind(partes[1] + SUFIXO_ARQUIVOS, partes[2] + SUFIXO_ARQUIVOS, "");
        break;

      case "removeGroup":
        if (partes.length != 2) {
          System.out.println("Uso: !removeGroup <grupo>");
          return;
        }
        channel.exchangeDelete(partes[1]);
        channel.exchangeDelete(partes[1] + SUFIXO_ARQUIVOS);
        break;

      case "upload":
        iniciarUpload(usuario, destino, comando.substring(partes[0].length()).trim());
        break;

      default:
        System.out.println("Comando desconhecido: !" + partes[0]);
    }
  }

  private static void iniciarUpload(String usuario, String destino, String caminho) {
    if (caminho.isEmpty()) {
      System.out.println("Uso: !upload <caminho-do-arquivo>");
      return;
    }
    if (destino == null) {
      System.out.println("Escolha um destinatário com @usuario ou #grupo antes de enviar arquivos.");
      return;
    }

    Path arquivo = resolverCaminho(caminho);
    if (!Files.isRegularFile(arquivo)) {
      System.out.println("Arquivo não encontrado: " + arquivo);
      return;
    }
    if (!Files.isReadable(arquivo)) {
      System.out.println("Sem permissão de leitura em: " + arquivo);
      return;
    }
    try {
      if (Files.size(arquivo) > TAMANHO_MAXIMO) {
        System.out.println("Arquivo maior que o limite de "
            + (TAMANHO_MAXIMO / 1024 / 1024) + " MB: " + arquivo);
        return;
      }
    } catch (IOException e) {
      System.out.println("Não foi possível ler " + arquivo + ": " + e.getMessage());
      return;
    }

    System.out.println("Enviando \"" + caminho + "\" para " + destino + ".");

    final String destinoFinal = destino;
    transferencias.submit(() -> enviarArquivo(usuario, destinoFinal, arquivo, caminho));
  }

  private static void enviarArquivo(String usuario, String destino, Path arquivo, String caminho) {
    boolean paraGrupo = destino.startsWith("#");
    String nomeDestino = destino.substring(1);
    String filaArquivos = nomeDestino + SUFIXO_ARQUIVOS;

    
    Channel canal = null;
    try {
      byte[] corpo = Files.readAllBytes(arquivo);
      String tipoMime = Files.probeContentType(arquivo);

      Mensagem mensagem = novaMensagem(usuario, paraGrupo ? nomeDestino : "")
          .setConteudo(Conteudo.newBuilder()
              .setTipo(tipoMime == null ? "application/octet-stream" : tipoMime)
              .setNome(arquivo.getFileName().toString())
              .setCorpo(ByteString.copyFrom(corpo)))
          .build();

      canal = connection.createChannel();
      canal.confirmSelect();

      if (paraGrupo) {
        canal.exchangeDeclarePassive(nomeDestino);
        canal.exchangeDeclare(filaArquivos, BuiltinExchangeType.FANOUT, true);
        canal.basicPublish(filaArquivos, "", null, mensagem.toByteArray());
      } else {
        canal.queueDeclare(filaArquivos, true, false, false, null);
        canal.basicPublish("", filaArquivos, null, mensagem.toByteArray());
      }

      canal.waitForConfirmsOrDie(60_000);
      exibir("Arquivo \"" + caminho + "\" foi enviado para " + destino + " !");
    } catch (Exception e) {
      exibir("Erro ao enviar \"" + caminho + "\" para " + destino + ": " + e.getMessage());
    } finally {
      fechar(canal);
    }
  }

  private static void receberArquivo(Mensagem mensagem) {
    Conteudo conteudo = mensagem.getConteudo();
    String nome = nomeSeguro(conteudo.getNome());
    try {
      Path destino = destinoLivre(PASTA_DOWNLOADS.resolve(nome));
      Files.write(destino, conteudo.getCorpo().toByteArray());
      exibir("(" + mensagem.getData() + " às " + mensagem.getHora() + ") Arquivo \""
          + destino.getFileName() + "\" recebido de " + origem(mensagem) + " !");
    } catch (Exception e) {
      exibir("Erro ao salvar \"" + nome + "\" recebido de "
          + origem(mensagem) + ": " + e.getMessage());
    }
  }

  private static Mensagem.Builder novaMensagem(String emissor, String grupo) {
    LocalDateTime agora = LocalDateTime.now();
    return Mensagem.newBuilder()
        .setEmissor(emissor)
        .setData(FORMATO_DATA.format(agora))
        .setHora(FORMATO_HORA.format(agora))
        .setGrupo(grupo);
  }

  private static String origem(Mensagem mensagem) {
    return mensagem.getGrupo().isEmpty()
        ? "@" + mensagem.getEmissor()
        : mensagem.getEmissor() + "#" + mensagem.getGrupo();
  }

  private static void exibir(String mensagem) {
    synchronized (System.out) {
      System.out.println();
      System.out.println(mensagem);
      System.out.print(prompt);
      System.out.flush();
    }
  }

  private static Path resolverCaminho(String caminho) {
    String limpo = caminho;
    if (limpo.length() > 1 && ((limpo.startsWith("\"") && limpo.endsWith("\""))
        || (limpo.startsWith("'") && limpo.endsWith("'")))) {
      limpo = limpo.substring(1, limpo.length() - 1);
    }
    if (limpo.equals("~") || limpo.startsWith("~/") || limpo.startsWith("~\\")) {
      limpo = System.getProperty("user.home") + limpo.substring(1);
    }
    return Paths.get(limpo);
  }

  private static String nomeSeguro(String nome) {
    String limpo = nome.replace('\\', '/');
    int barra = limpo.lastIndexOf('/');
    if (barra >= 0) limpo = limpo.substring(barra + 1);
    limpo = limpo.trim();
    return (limpo.isEmpty() || limpo.equals(".") || limpo.equals("..")) ? "arquivo" : limpo;
  }

  private static Path destinoLivre(Path desejado) {
    if (!Files.exists(desejado)) return desejado;

    String nome = desejado.getFileName().toString();
    int ponto = nome.lastIndexOf('.');
    String base = ponto > 0 ? nome.substring(0, ponto) : nome;
    String extensao = ponto > 0 ? nome.substring(ponto) : "";

    for (int i = 1; i < 1000; i++) {
      Path alternativo = desejado.resolveSibling(base + "(" + i + ")" + extensao);
      if (!Files.exists(alternativo)) return alternativo;
    }
    return desejado.resolveSibling(base + "-" + System.currentTimeMillis() + extensao);
  }

  private static Path pastaDownloads() {
    String configurada = System.getenv("CHAT_DOWNLOADS");
    if (configurada != null && !configurada.isEmpty()) return Paths.get(configurada);
    return Paths.get(System.getProperty("user.home"), "chat", "downloads");
  }

  private static void fechar(Channel canal) {
    if (canal == null || !canal.isOpen()) return;
    try {
      canal.close();
    } catch (Exception e) {
    }
  }

  private static String env(String nome, String padrao) {
    String v = System.getenv(nome);
    return (v == null || v.isEmpty()) ? padrao : v;
  }
}
