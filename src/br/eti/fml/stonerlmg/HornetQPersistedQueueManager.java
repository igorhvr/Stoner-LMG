/*
 * Copyright (c) 2010.
 * CC-by Felipe Micaroni Lalli <micaroni@gmail.com>
 * Special thanks to Igor Hjelmstrom Vinhas Ribeiro <igorhvr@iasylum.net>
 */

package br.eti.fml.stonerlmg;

import br.fml.eti.machinegun.auditorship.ArmyAudit;
import br.fml.eti.machinegun.externaltools.Consumer;
import br.fml.eti.machinegun.externaltools.PersistedQueueManager;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.server.impl.HornetQServerImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * HornetQ implementation of
 * {@link br.fml.eti.machinegun.externaltools.PersistedQueueManager}.
 *
 * @author Felipe Micaroni Lalli (micaroni@gmail.com)
 *         Nov 21, 2010 3:23:28 AM
 */
public class HornetQPersistedQueueManager implements PersistedQueueManager {
    private HornetQServerImpl server;

    private Map<String, ClientSession> sessions = new HashMap<String, ClientSession>();
    private Map<String, ClientProducer> producers = new HashMap<String, ClientProducer>();
    private Map<String, ClientConsumer> consumers = new HashMap<String, ClientConsumer>();

    public HornetQPersistedQueueManager() throws Exception {
        Configuration config = new ConfigurationImpl();
        HashSet<TransportConfiguration> transports = new HashSet<TransportConfiguration>();
        transports.add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
        transports.add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
        config.setAcceptorConfigurations(transports);

        this.server = new HornetQServerImpl(config);
        this.server.start();
    }

    private synchronized ClientSession getSession(String queueName) throws HornetQException {
        if (!this.sessions.containsKey(queueName)) {
            ClientSessionFactory nettyFactory
                    = HornetQClient.createClientSessionFactory(
                            new TransportConfiguration(
                                    InVMConnectorFactory.class.getName()));

            ClientSession session = nettyFactory.createSession();
            session.createQueue(queueName, queueName, true);
            session.start();

            ClientProducer clientProducer = session.createProducer(queueName);
            ClientConsumer clientConsumer = session.createConsumer(queueName);

            this.producers.put(queueName, clientProducer);
            this.consumers.put(queueName, clientConsumer);
            this.sessions.put(queueName, session);
        }

        return this.sessions.get(queueName);
    }

    private ClientProducer getProducer(String queue) throws HornetQException {
        return this.producers.get(queue);
    }

    private ClientConsumer getConsumer(String queue) throws HornetQException {
        return this.consumers.get(queue);
    }

    @Override
    public void putIntoAnEmbeddedQueue(ArmyAudit armyAudit,
                                       String queue, byte[] bytes) throws InterruptedException {

        try {
            ClientSession session = getSession(queue);
            ClientProducer producer = getProducer(queue);
            ClientMessage message = session.createMessage(true);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            message.setBodyInputStream(byteArrayInputStream);
            producer.send(message);
            session.commit();
        } catch (Exception e) {
            armyAudit.errorWhenPuttingIntoAnEmbeddedQueue(e);
        }
    }

    @Override
    public void registerANewConsumerInAnEmbeddedQueue(
            final ArmyAudit armyAudit, final String queue, final Consumer consumer) {

        try {
            ClientSession session = getSession(queue);
            ClientConsumer clientConsumer = getConsumer(queue);

            clientConsumer.setMessageHandler(new MessageHandler() {

                @Override
                public void onMessage(ClientMessage clientMessage) {
                    ByteArrayOutputStream byteArrayOutputStream
                            = new ByteArrayOutputStream();

                    try {
                        clientMessage.saveToOutputStream(byteArrayOutputStream);
                        consumer.consume(byteArrayOutputStream.toByteArray());
                    } catch (Exception e) {
                        armyAudit.errorWhenRegisteringANewConsumerInAnEmbeddedQueue(e);
                    }
                }
            });
        } catch (Exception e) {
            armyAudit.errorWhenRegisteringANewConsumerInAnEmbeddedQueue(e);            
        }
    }

    @Override
    public void killAllConsumers(String queueName) throws InterruptedException {
        try {
            getSession(queueName).close();
        } catch (HornetQException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() throws Exception {
        this.server.stop();
    }
}
