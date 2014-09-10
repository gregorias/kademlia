package org.nebulostore.dfuntesting;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.kademlia.interfaces.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KademliaEnvironmentPreparator implements EnvironmentPreparator {
  public static final String XML_CONFIG_FILENAME = "kademlia.xml";
  public static final Path XML_CONFIG_PATH = FileSystems.getDefault().getPath(XML_CONFIG_FILENAME);
  public static final Path XML_JAR_PATH = FileSystems.getDefault().getPath("kademlia.jar");
  public static final Path XML_LIBS_PATH = FileSystems.getDefault().getPath("allLibs");
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaEnvironmentPreparator.class);
  private final int mInitialPort;
  private final int mInitialRestPort;
  private final int mBucketSize;
  private final boolean mUseDifferentPorts;

  public KademliaEnvironmentPreparator(int initialPort, int initialRestPort, int bucketSize,
      boolean useDifferentPorts) {
    mInitialPort = initialPort;
    mInitialRestPort = initialRestPort;
    mBucketSize = bucketSize;
    mUseDifferentPorts = useDifferentPorts;
  }

  @Override
  public void cleanEnvironments(Collection<Environment> envs) {
    for (Environment env : envs) {
      Path localPath = FileSystems.getDefault().getPath(".");
      Path kademliaXMLPath = localPath.resolve(XML_CONFIG_PATH);
      try {
        env.removeFile(kademliaXMLPath);
      } catch (IOException e) {
        LOGGER.error("cleanEnvironments(): Could not clean environment.", e);
      }
    }
  }

  @Override
  public void collectOutputAndLogFiles(Collection<Environment> envs) {
    // TODO
  }

  @Override
  public void prepareEnvironments(Collection<Environment> envs) throws ExecutionException {
    Collection<Environment> preparedEnvs = new LinkedList<>();
    LOGGER.info("prepareEnvironments()");
    Environment zeroEnvironment = findZeroEnvironment(envs);
    for (Environment env : envs) {
      XMLConfiguration xmlConfig = prepareXMLAndEnvConfiguration(env, zeroEnvironment);
      try {
        xmlConfig.save(XML_CONFIG_FILENAME);
        Path targetPath = FileSystems.getDefault().getPath(".");
        env.copyFilesFromLocalDisk(XML_CONFIG_PATH.toAbsolutePath(), targetPath);
        env.copyFilesFromLocalDisk(XML_JAR_PATH.toAbsolutePath(), targetPath);
        env.copyFilesFromLocalDisk(XML_LIBS_PATH.toAbsolutePath(), targetPath);
        preparedEnvs.add(env);
      } catch (ConfigurationException | IOException e) {
        cleanEnvironments(preparedEnvs);
        LOGGER.error("prepareEnvironments() -> Could not prepare environment.", e);
        throw new ExecutionException(e);
      }
    }
  }

  private Environment findZeroEnvironment(Collection<Environment> envs) throws
      NoSuchElementException {
    for (Environment env : envs) {
      if (env.getId() == 0) {
        return env;
      }
    }
    throw new NoSuchElementException("No zero environment present");
  }

  private XMLConfiguration prepareXMLAndEnvConfiguration(Environment env, Environment zeroEnv) {
    XMLConfiguration xmlConfiguration = new XMLConfiguration();
    int portSkew = mUseDifferentPorts ? env.getId() : 0;
    xmlConfiguration.addProperty(Main.XML_FIELD_LOCAL_ADDRESS, env.getHostname());
    xmlConfiguration.addProperty(Main.XML_FIELD_LOCAL_PORT, mInitialPort + portSkew);
    xmlConfiguration.addProperty(Main.XML_FIELD_LOCAL_KEY, env.getId());
    xmlConfiguration.addProperty(Main.XML_FIELD_BUCKET_SIZE, mBucketSize);
    xmlConfiguration.addProperty(Main.XML_FIELD_BOOTSTRAP_KEY, 0);
    xmlConfiguration.addProperty(Main.XML_FIELD_BOOTSTRAP_ADDRESS, zeroEnv.getHostname());
    xmlConfiguration.addProperty(Main.XML_FIELD_BOOTSTRAP_PORT, mInitialPort);
    xmlConfiguration.addProperty(Main.XML_FIELD_REST_PORT, mInitialRestPort + portSkew);
    env.setProperty(Main.XML_FIELD_LOCAL_ADDRESS, env.getHostname());
    env.setProperty(Main.XML_FIELD_REST_PORT, mInitialRestPort + portSkew);
    return xmlConfiguration;
  }

}
