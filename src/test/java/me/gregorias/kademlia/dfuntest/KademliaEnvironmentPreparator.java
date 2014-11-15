package me.gregorias.kademlia.dfuntest;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.kademlia.interfaces.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preparator of testing environments for Kademlia tests.
 *
 * This preparator prepares XML configuration file expected by {@link Main} and sets up environment
 * properties for {@link KademliaAppFactory}.
 *
 * It assumes that:
 * <ul>
 * <li> All required dependency libraries are in lib/ directory. </li>
 * <li> Kademlia is in kademlia.jar file. </li>
 * </ul>
 *
 * @author Grzegorz Milka
 */
public class KademliaEnvironmentPreparator implements EnvironmentPreparator {
  private static final String XML_CONFIG_FILENAME = "kademlia.xml";
  private static final String LOCAL_CONFIG_PATH = XML_CONFIG_FILENAME;
  private static final Path LOCAL_JAR_PATH = FileSystems.getDefault().getPath("kademlia.jar");
  private static final Path LOCAL_LIBS_PATH = FileSystems.getDefault().getPath("lib");
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaEnvironmentPreparator.class);
  private final int mInitialPort;
  private final int mInitialRestPort;
  private final int mBucketSize;
  private final boolean mUseDifferentPorts;
  private final Path mReportPath;

  /**
   * @param initialPort Port number used by first kademlia application for kademlia.
   * @param initialRestPort Port number used by first kademlia application for REST.
   * @param bucketSize BucketSize of each kademlia peer.
   * @param useDifferentPorts Whether each kademlia peer should use a different port.
   * Used when all peers are on the same host.
   * @param reportPath Path where logs will be placed in consecutive directories.
   */
  public KademliaEnvironmentPreparator(int initialPort,
      int initialRestPort,
      int bucketSize,
      boolean useDifferentPorts,
      Path reportPath) {
    mInitialPort = initialPort;
    mInitialRestPort = initialRestPort;
    mBucketSize = bucketSize;
    mUseDifferentPorts = useDifferentPorts;
    mReportPath = reportPath;
  }

  @Override
  public void cleanEnvironments(Collection<Environment> envs) {
    String logFilePath = getLogFilePath();
    for (Environment env : envs) {
      try {
        env.removeFile(LOCAL_CONFIG_PATH);
        env.removeFile(logFilePath);
      } catch (IOException e) {
        LOGGER.error("cleanEnvironments(): Could not clean environment.", e);
      } catch (InterruptedException e) {
        LOGGER.warn("cleanEnvironments(): Could not clean environment.", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void collectOutputAndLogFiles(Collection<Environment> envs) {
    String logFilePath = getLogFilePath();
    for (Environment env : envs) {
      try {
        env.copyFilesToLocalDisk(logFilePath, mReportPath.resolve(env.getId() + ""));
      } catch (IOException e) {
        LOGGER.warn("collectOutputAndLogFiles(): Could not copy log file.", e);
      }
    }
  }

  /**
   * This method expects that environments are numbered from 0.
   * Zero environment will be configured as kademlia bootstrap server.
   */
  @Override
  public void prepareEnvironments(Collection<Environment> envs) throws IOException {
    Collection<Environment> preparedEnvs = new LinkedList<>();
    LOGGER.info("prepareEnvironments()");
    Environment zeroEnvironment = findZeroEnvironment(envs);
    for (Environment env : envs) {
      XMLConfiguration xmlConfig = prepareXMLAndEnvConfiguration(env, zeroEnvironment);
      try {
        xmlConfig.save(XML_CONFIG_FILENAME);
        String targetPath = ".";
        Path localConfigPath = FileSystems.getDefault().getPath(LOCAL_CONFIG_PATH).toAbsolutePath();
        env.copyFilesFromLocalDisk(localConfigPath, targetPath);
        env.copyFilesFromLocalDisk(LOCAL_JAR_PATH.toAbsolutePath(), targetPath);
        env.copyFilesFromLocalDisk(LOCAL_LIBS_PATH.toAbsolutePath(), targetPath);
        preparedEnvs.add(env);
      } catch (ConfigurationException | IOException e) {
        cleanEnvironments(preparedEnvs);
        LOGGER.error("prepareEnvironments() -> Could not prepare environment.", e);
        throw new IOException(e);
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

  private String getLogFilePath() {
    return "./" + KademliaApp.LOG_FILE;
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
