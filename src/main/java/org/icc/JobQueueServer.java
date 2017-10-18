package org.icc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.icc.kafka.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobQueueServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobQueueServer.class);

    public static final String JOB_SERVER_PORT_SHORT_OPT = "p";
    public static final String JOB_SERVER_PORT_HOSTS_OPT = "port";

    public static final String KAFKA_TOPIC_SHORT_OPT = "t";
    public static final String KAFKA_TOPIC_HOSTS_OPT = "topic";

    public static final String KAFKA_ZOOKEEPER_HOSTS_SHORT_OPT = "z";
    public static final String KAFKA_ZOOKEEPER_HOSTS_OPT = "zookeeper";

    public static final String KAFKA_BROKER_HOSTS_SHORT_OPT = "b";
    public static final String KAFKA_BROKER_HOSTS_OPT = "broker";

    private static Integer JOB_SERVER_PORT = 9999;
    private static String KAFKA_TOPIC = "test-topic";
    private static String KAFKA_ZOOKEEPER_HOSTS = "localhost:2181";;
    private static String KAFKA_BROKER_HOSTS = "localhost:9092";
    private static Producer myProducer = null;
    private Server jettyServer;

    public static void main(String[] args) {
        JobQueueServer jobQueueServer = new JobQueueServer();
        //try{
            //Options opts = getOptions();
            //CommandLineParser parser = new GnuParser();
            //CommandLine line = parser.parse(opts, args, true);
            //jobQueueServer.processOptions(line);

        //} catch (ParseException e) {
        //    System.out.println(e.getMessage());
        //    System.out.println("Try \"--help\" option for details.");
        //    System.exit(0);
        //}
        jobQueueServer.init();
        jobQueueServer.start();
    }

    public void init() {
        if( myProducer == null) {
            myProducer= new Producer(KAFKA_TOPIC, KAFKA_ZOOKEEPER_HOSTS, KAFKA_BROKER_HOSTS);
        }
    }

    public static Producer getProducer() {
        return myProducer;
    }

    public void start() {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");
        jettyServer = new Server(JOB_SERVER_PORT);
        jettyServer.setHandler(context);
        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "org.icc");
        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processOptions(CommandLine line) throws ParseException {
        if (line.hasOption('p')) {
            JOB_SERVER_PORT = Integer.parseInt(line.getOptionValue('t'));
        }

        if (line.hasOption('t')) {
            KAFKA_TOPIC = line.getOptionValue('t');
        }

        if (line.hasOption('z')) {
            KAFKA_ZOOKEEPER_HOSTS = line.getOptionValue('z');
        }

        if (line.hasOption('b')) {
            KAFKA_BROKER_HOSTS = line.getOptionValue('b');
        }
    }

    private static Options getOptions() {
        Options options = new Options();

        Option jobServerPort = new Option(JOB_SERVER_PORT_SHORT_OPT, JOB_SERVER_PORT_HOSTS_OPT, true,"Job server port on it listning.");
        jobServerPort.setRequired(true);
        jobServerPort.setArgName("JOB Server Port");
        options.addOption(jobServerPort);

        Option kafkaTopic = new Option(KAFKA_TOPIC_SHORT_OPT, KAFKA_TOPIC_HOSTS_OPT, true,"kafka topic.");
        kafkaTopic.setRequired(true);
        kafkaTopic.setArgName("Kafak toic.");
        options.addOption(kafkaTopic);

        Option zookeeperHosts = new Option(KAFKA_ZOOKEEPER_HOSTS_SHORT_OPT, KAFKA_ZOOKEEPER_HOSTS_OPT, true,"List of Zookeeper Hosts with comma seprated.");
        zookeeperHosts.setRequired(true);
        zookeeperHosts.setArgName("Zookeeper Hosts.");
        options.addOption(zookeeperHosts);

        Option brokerHosts = new Option(KAFKA_BROKER_HOSTS_SHORT_OPT, KAFKA_BROKER_HOSTS_OPT, true,"List of Broker Hosts with comma seprated.");
        brokerHosts.setRequired(true);
        brokerHosts.setArgName("Broker Hosts.");
        options.addOption(brokerHosts);

        return options;
    }

    public void stop() {
        jettyServer.destroy();
    }
}