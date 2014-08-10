package org.nebulostore.dfuntesting;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.dfuntesting.TestResult.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestRunner for {@link KademliaConsistencyCheckTestScript}
 * 
 * @author Grzegorz Milka
 */
public class KademliaConsistencyCheckTestRunner extends
    SingleTestRunner<KademliaApp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaConsistencyCheckTestRunner.class);
  private static final String XML_ENV_FACTORY_CLASS_FIELD = "environment-factory-class-name";
  private static final String XML_ENV_FACTORY_CONFIG_FIELD = "environment-factory-config";
  private static final String XML_FIELD_BUCKET_SIZE = "bucket-size";
  private static final String XML_FIELD_INITIAL_PORT = "initial-kademlia-port";
  private static final String XML_FIELD_REST_PORT = "initial-rest-port";
  private static final String XML_FIELD_SHOULD_USE_DIFFERENT_PORTS = "should-use-different-ports";
  
  public KademliaConsistencyCheckTestRunner(ScheduledExecutorService scheduledExecutor,
      ExecutorService executor,
      EnvironmentFactory environmentFactory,
      int initialPort,
      int initialKademliaPort,
      int bucketSize,
      boolean shouldUseDifferentPorts) {
    super(new KademliaConsistencyCheckTestScript(scheduledExecutor),
        LOGGER, 
        environmentFactory,
        new KademliaEnvironmentPreparator(initialPort, initialKademliaPort, bucketSize, shouldUseDifferentPorts),
        new LocalKademliaAppFactory());
  }

  /**
   * Initializes this test runner based on given config filename.
   * 
   * To run test from this Runner you must provide filename of the XML configuration file as argument.
   * Configuration file should contain fields/trees:
   *   environment-factory-class-name - class name of environment factory that should be used
   *   environment-factory-config     - config which will be given to the environment factory
   * 
   * @param args
   */
  public static void main(String []args) {
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
    } catch (InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      LOGGER.error("main(): Could not create instance of {}.", className, e);
      return;
    }
    
    int initialKademliaPort = testConfiguration.getInt(XML_FIELD_INITIAL_PORT);
    int initialRESTPort = testConfiguration.getInt(XML_FIELD_REST_PORT);
    int bucketSize = testConfiguration.getInt(XML_FIELD_BUCKET_SIZE);
    boolean shouldUseDiffentPorts = testConfiguration.getBoolean(XML_FIELD_SHOULD_USE_DIFFERENT_PORTS, true);


    KademliaConsistencyCheckTestRunner testRunner = 
        new KademliaConsistencyCheckTestRunner(scheduledExecutor,
          executor,
          environmentFactory,
          initialKademliaPort,
          initialRESTPort,
          bucketSize,
          shouldUseDiffentPorts);
    
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
    LOGGER.info("main(): Test has ended with description: {}", resultStr, result.getDescription());
    System.exit(status);
  }
}
