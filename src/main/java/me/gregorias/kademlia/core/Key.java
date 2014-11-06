package me.gregorias.kademlia.core;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

/**
 * Immutable 160 bit long Kademlia key.
 *
 * @author Grzegorz Milka
 */
public class Key implements Serializable {
  public static final int KEY_LENGTH = 160;
  public static final int HEX = 16;
  private static final long serialVersionUID = 1L;
  private static final int BINARY = 2;
  private final BitSet mKey;

  public Key(BitSet key) {
    assert key.length() <= KEY_LENGTH;
    mKey = (BitSet) key.clone();
  }

  /**
   * Creates a key from integer in a little-endian bit fashion.
   *
   * @param key
   *          nonnegative number
   * @throws IllegalArgumentException
   */
  public Key(int key) throws IllegalArgumentException {
    if (key < 0) {
      throw new IllegalArgumentException("Key should be a nonnegative number.");
    }
    BitSet bitSet = new BitSet(KEY_LENGTH);
    for (int idx = 0; key > 0; ++idx) {
      if (key % 2 != 0) {
        bitSet.set(idx);
      }
      key /= 2;
    }
    mKey = bitSet;
    assert mKey.length() <= KEY_LENGTH;
  }

  /**
   * Creates a key from a string representing hex number.
   *
   * @param key
   *          hexadecimal number in string
   * @throws IllegalArgumentException
   */
  public Key(String key) {
    this(Integer.parseInt(key, HEX));
  }

  static Key newRandomKey(Random random) {
    BitSet generatedBitSet = new BitSet(KEY_LENGTH);
    for (int bitIdx = 0; bitIdx < KEY_LENGTH; ++bitIdx) {
      if (random.nextBoolean()) {
        generatedBitSet.set(bitIdx);
      }
    }

    return new Key(generatedBitSet);
  }

  static Key xor(Key firstKey, Key secondKey) {
    BitSet newKey = firstKey.getBitSet();
    newKey.xor(secondKey.mKey);
    return new Key(newKey);
  }

  /**
   * @return distance {@link BitSet} between two keys in little-endian encoding.
   */
  BitSet calculateDistance(Key otherKey) {
    return xor(otherKey).mKey;
  }

  /**
   * Following assertion is true: assert (new Key(1)).getDistanceBit(new Key(2))
   * == 1
   *
   * @return most significant bit index of distance between this key and
   *         argument.
   */
  int getDistanceBit(Key otherKey) {
    BitSet distance = otherKey.calculateDistance(this);
    return distance.previousSetBit(distance.length() - 1);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Key)) {
      return false;
    }
    Key other = (Key) obj;
    if (mKey == null) {
      if (other.mKey != null) {
        return false;
      }
    } else if (!mKey.equals(other.mKey)) {
      return false;
    }
    return true;
  }

  /**
   * @return little-endian encoding of this key
   */
  public BitSet getBitSet() {
    return (BitSet) mKey.clone();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mKey == null) ? 0 : mKey.hashCode());
    return result;
  }

  public BigInteger toInt() {
    StringBuilder strBuilder = new StringBuilder(KEY_LENGTH);
    for (int keyIdx = KEY_LENGTH - 1; keyIdx >= 0; --keyIdx) {
      if (mKey.get(keyIdx)) {
        strBuilder.append("1");
      } else {
        strBuilder.append("0");
      }
    }
    BigInteger bigInteger = new BigInteger(strBuilder.toString(), BINARY);
    return bigInteger;
  }

  @Override
  public String toString() {
    return toInt().toString(HEX);
  }

  Key xor(Key otherKey) {
    return xor(this, otherKey);
  }
}
