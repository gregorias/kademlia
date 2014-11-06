package me.gregorias.kademlia.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serialization utilities for messages using Java's serialization mechanisms.
 *
 * @author Grzegorz Milka
 */
final class MessageSerializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageSerializer.class);

  public static Message translateFromByteToMessage(byte[] byteMsg) {
    Message msg = null;
    InputStream bais = new ByteArrayInputStream(byteMsg);
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(bais);
      msg = (Message) ois.readObject();
    } catch (ClassCastException | ClassNotFoundException e) {
      LOGGER.warn("translateFromByteToMessage() -> Could not deserialize message", e);
    } catch (IOException e) {
      LOGGER.error("translateFromByteToMessage() -> Caught unexpected IOException", e);
    } finally {
      try {
        if (ois != null) {
          ois.close();
        }
      } catch (IOException e) {
        LOGGER.error("translateFromByteToMessage() -> Caught unexpected"
            + " IOException during closing of stream", e);
      } finally {
        try {
          bais.close();
        } catch (IOException e) {
          LOGGER.error("translateFromByteToMessage() -> Caught unexpected"
              + " IOException during closing of byte stream", e);
        }
      }
    }
    return msg;
  }

  public static byte[] translateFromMessageToByte(Message message) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(baos);
      oos.writeObject(message);
    } catch (IOException e) {
      LOGGER.error("translateFromMessageToByte() -> Caught unexpected IOException", e);
      return null;
    } finally {
      try {
        if (oos != null) {
          oos.close();
        }
      } catch (IOException e) {
        LOGGER.error("translateFromMessageToByte() -> Caught unexpected"
            + " IOException during closing of stream", e);
        return null;
      } finally {
        try {
          baos.close();
        } catch (IOException e) {
          LOGGER.error("translateFromMessageToByte() -> Caught unexpected"
              + " IOException during closing of byte stream", e);
          return null;
        }
      }
    }
    return baos.toByteArray();
  }
}
