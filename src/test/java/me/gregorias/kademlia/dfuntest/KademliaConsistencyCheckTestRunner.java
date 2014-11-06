package me.gregorias.kademlia.dfuntest;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import me.gregorias.dfuntest.EnvironmentFactory;
import me.gregorias.dfuntest.SingleTestRunner;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestRunner for {@link KademliaConsistencyCheckTestScript}.
 *
 * This is the main entry point for Consistency Check test. The main method accepts
 * configuration filename.
 *
 * The configuration filename should contain following fields:
 * <ul>
 * <li> environment-factory-class-name - Class name of factory which will initialize
 * environments. </li>
 * <li> environment-factory-config - Subconfiguration which will be given to
 * {@link EnvironmentFactory} </li>
 * <li> bucket-size - Default size of bucket. </li>
 * <li> initial-kademlia-port - Initial port number used by first peer for kademlia
 * communication. </li>
 * <li> initial-rest-port - As above but for REST. </li>
 * <li> should-use-different-ports - Whether this test should use different port numbers for each
 * application, for example when some peers are on the same host. </li>
 * <li> java-command - command which starts java on remote host. </li>
 * </ul>
 *
 * @author Grzegorz Milka
 */
public class KademliaConsistencyCheckTestRunner extends SingleTestRunner<KademliaApp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      KademliaConsistencyCheckTestRunner.class);
  private static final String XML_ENV_FACTORY_CLASS_FIELD = "environment-factory-class-name";
  private static final String XML_ENV_FACTORY_CONFIG_FIELD = "environment-factory-config";
  private static final String XML_FIELD_BUCKET_SIZE = "bucket-size";
  private static final String XML_FIELD_INITIAL_PORT = "initial-kademlia-port";
  private static final String XML_FIELD_REST_PORT = "initial-rest-port";
  private static final String XML_FIELD_SHOULD_USE_DIFFERENT_PORTS = "should-use-different-ports";
  private static final String XML_FIELD_JAVA_COMMAND = "java-command";
  private static final Path REPORT_PATH = FileSystems.getDefault().getPath("report");

  public KademliaConsistencyCheckTestRunner(ScheduledExecutorService scheduledExecutor,
      ExecutorService executor, EnvironmentFactory environmentFactory, int initialPort,
      int initialKademliaPort, int bucketSize, boolean shouldUseDifferentPorts,
      String javaCommand) {
    super(new KademliaConsistencyCheckTestScript(scheduledExecutor), LOGGER, environmentFactory,
        new KademliaEnvironmentPreparator(initialPort, initialKademliaPort, bucketSize,
            shouldUseDifferentPorts, REPORT_PATH), new KademliaAppFactory(javaCommand));
  }

  /**
   * Initializes this test runner based on given config filename.
   *
   * To run test from this Runner you must provide filename of the XML
   * configuration file as argument. Configuration file should contain
   * fields/trees: environment-factory-class-name - class name of environment
   * factory that should be used environment-factory-config - config which will
   * be given to the environment factory
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("Usage: KademliaConsistencyCheckTestRunner CONFIG_FILE");
      System.exit(1);
    }

    Configuration testConfiguration = null;
    try {
      testConfiguration = new XMLConfiguration(args[0]);
    } catch (ConfigurationException e) {
      LOGGER.error("main(): Could not create configuration.", e);
      System.exit(1);
    }

    String className = testConfiguration.getString(XML_ENV_FACTORY_CLASS_FIELD);
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    ExecutorService executor = Executors.newCachedThreadPool();
    Class<?> environmentFactoryClass;
    try {
      environmentFactoryClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOGGER.error("main(): Could not find EnvironmentFactory class {}.", className, e);
      System.exit(1);
      return;
    }

    Constructor<?> constructor;
    try {
      constructor = environmentFactoryClass.getConstructor(Configuration.class);
    } catch (NoSuchMethodException | SecurityException e) {
      LOGGER.error("main(): Could not get constructor of {}.", className, e);
      System.exit(1);
      return;
    }

    Configuration envFactoryConfig = testConfiguration.subset(XML_ENV_FACTORY_CONFIG_FIELD);
    if (envFactoryConfig.isEmpty()) {
      LOGGER.error("main(): Could not get EnvironmentFactory configuration.");
      System.exit(1);
      return;
    }

    EnvironmentFactory environmentFactory;
    try {
      environmentFactory = (EnvironmentFactory) constructor.newInstance(envFactoryConfig);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      LOGGER.error("main(): Could not create instance of {}.", className, e);
      return;
    }

    int initialKademliaPort = testConfiguration.getInt(XML_FIELD_INITIAL_PORT);
    int initialRESTPort = testConfiguration.getInt(XML_FIELD_REST_PORT);
    int bucketSize = testConfiguration.getInt(XML_FIELD_BUCKET_SIZE);
    boolean shouldUseDiffentPorts = testConfiguration.getBoolean(
      XML_FIELD_SHOULD_USE_DIFFERENT_PORTS, true);
    String javaCommand = testConfiguration.getString(XML_FIELD_JAVA_COMMAND, "java");

    KademliaConsistencyCheckTestRunner testRunner = new KademliaConsistencyCheckTestRunner(
        scheduledExecutor, executor, environmentFactory, initialKademliaPort, initialRESTPort,
        bucketSize, shouldUseDiffentPorts, javaCommand);

    TestResult result = testRunner.run();
    int status;
    String resultStr;
    if (result.getType() == Type.SUCCESS) {
      status = 0;
      resultStr = "successfully";
    } else {
      status = 1;
      resultStr = "with failure";
    }
    LOGGER.info("main(): Test has ended {} with description: {}", resultStr,
        result.getDescription());
    System.exit(status);
  }
}
