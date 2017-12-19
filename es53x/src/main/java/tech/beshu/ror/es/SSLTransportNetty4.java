/*
 *    This file is part of ReadonlyREST.
 *
 *    ReadonlyREST is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    ReadonlyREST is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with ReadonlyREST.  If not, see http://www.gnu.org/licenses/
 */

package tech.beshu.ror.es;

/**
 * Created by sscarduzio on 28/11/2016.
 */

import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import tech.beshu.ror.commons.SSLCertParser;
import tech.beshu.ror.commons.settings.BasicSettings;
import tech.beshu.ror.commons.shims.es.LoggerShim;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class SSLTransportNetty4 extends Netty4HttpServerTransport {

  private final BasicSettings basicSettings;
  private final LoggerShim logger;

  public SSLTransportNetty4(Settings settings, NetworkService networkService, BigArrays bigArrays,
                            ThreadPool threadPool, NamedXContentRegistry xContentRegistry, Dispatcher dispatcher) {
    super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher);
    this.logger = ESContextImpl.mkLoggerShim(Loggers.getLogger(getClass().getName()));

    Environment env = new Environment(settings);
    BasicSettings baseSettings = BasicSettings.fromFile(logger, env.configFile().toAbsolutePath(), settings.getAsStructuredMap());
    this.basicSettings = baseSettings;
    if (basicSettings.isSSLEnabled()) {
      logger.info("creating SSL transport");
    }
  }

  protected void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    if (!this.lifecycle.started()) {
      return;
    }
    if (cause.getCause() instanceof NotSslRecordException) {
      logger.warn(cause.getMessage());
    }
    else {
      cause.printStackTrace();
      super.exceptionCaught(ctx, cause);
    }
    ctx.channel().flush().close();
  }

  public ChannelHandler configureServerChannelHandler() {
    SSLHandler handler = new SSLHandler(this);
    logger.info("ROR SSL accepted ciphers: " + Joiner.on(",").join(handler.context.get().cipherSuites()));
    return handler;
  }

  private class SSLHandler extends Netty4HttpServerTransport.HttpChannelHandler {
    private Optional<SslContext> context = Optional.empty();

    SSLHandler(final Netty4HttpServerTransport transport) {
      super(transport, SSLTransportNetty4.this.detailedErrorsEnabled, SSLTransportNetty4.this.threadPool.getThreadContext());

      new SSLCertParser(basicSettings, logger, (certChain, privateKey) -> {
        try {
          // #TODO expose configuration of sslPrivKeyPem password? Letsencrypt never sets one..
          SslContextBuilder sslcb = SslContextBuilder.forServer(
            new ByteArrayInputStream(certChain.getBytes(StandardCharsets.UTF_8)),
            new ByteArrayInputStream(privateKey.getBytes(StandardCharsets.UTF_8)),
            null
          );

          if (basicSettings.getAllowedSSLCiphers().isPresent()) {
            sslcb.ciphers(basicSettings.getAllowedSSLCiphers().get());
          }

          if (basicSettings.getAllowedSSLProtocols().isPresent()) {
            List<String> protocols = basicSettings.getAllowedSSLProtocols().get();
            sslcb.applicationProtocolConfig(new ApplicationProtocolConfig(
              ApplicationProtocolConfig.Protocol.NPN_AND_ALPN,
              ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
              ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
              protocols
            ));

            logger.info("ROR SSL accepted protocols: " + Joiner.on(",").join(protocols));
          }

          SslContext sslContext = sslcb.build();
          context = Optional.of(sslContext);

        } catch (Exception e) {
          context = Optional.empty();
          logger.error("Failed to load SSL CertChain & private key from Keystore!");
          e.printStackTrace();
        }
      });
    }

    protected void initChannel(final Channel ch) throws Exception {
      super.initChannel(ch);
      context.ifPresent(sslCtx -> {
        ch.pipeline().addFirst("ssl_netty4_handler", sslCtx.newHandler(ch.alloc()));
      });
    }
  }
}
