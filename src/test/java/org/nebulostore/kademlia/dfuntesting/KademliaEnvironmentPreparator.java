package org.nebulostore.kademlia.dfuntesting;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.dfuntesting.Environment;
import org.nebulostore.dfuntesting.EnvironmentPreparator;
import org.nebulostore.kademlia.interfaces.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preparator of testing environments for Kademlia tests.
 *
 * This preparator prepares XML configuration file expected by {@link Main} and sets up environment
 * properties for {@link KademliaAppFactory}.
 *
 * It expects that all required libraries are in allLibs directory and kademlia itself is in
 * kademlia.jar file.
 *
 * @author Grzegorz Milka
 */
public class KademliaEnvironmentPreparator implements EnvironmentPreparator {
  private static final String XML_CONFIG_FILENAME = "kademlia.xml";
  private static final Path XML_CONFIG_PATH = FileSystems.getDefault().getPath(XML_CONFIG_FILENAME);
  private static final Path XML_JAR_PATH = FileSystems.getDefault().getPath("kademlia.jar");
  private static final Path XML_LIBS_PATH = FileSystems.getDefault().getPath("allLibs");
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaEnvironmentPreparator.class);
  private final int mInitialPort;
  private final int mInitialRestPort;
  private final int mBucketSize;
  private final boolean mUseDifferentPorts;

  /**
   *
   * @param initialPort Port number used by first kademlia application for kademlia.
   * @param initialRestPort Port number used by first kademlia application for REST.
   * @param bucketSize BucketSize of each kademlia peer.
   * @param useDifferentPorts Whether each kademlia peer should use a different port.
   * Used when all peers are on the same host.
   */
  public KademliaEnvironmentPreparator(int initialPort,
      int initialRestPort,
      int bucketSize,
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

  /**
   * This method expects that environments are numbered from 0.
   * Zero environment will be configured as kademlia bootstrap server.
   */
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
