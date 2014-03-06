package org.nebulostore.kademlia.core;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

/**
 * Immutable 160 bit long Kademlia key.
 * 
 * @author Grzegorz Milka
 *
 */
public class Key implements Serializable {
	public static final int KEY_LENGTH = 160;
	private static final long serialVersionUID = 1L;
	private static final int BINARY = 2;
	private static final int HEX = 16;
	private final BitSet key_;
	
	public Key(BitSet key) {
		assert key.length() <= KEY_LENGTH;
		key_ = (BitSet) key.clone();
	}

	public Key(int key) {
		if (key < 0) {
			throw new IllegalArgumentException("Key should be a nonnegative number.");
		}
		BitSet bitSet = new BitSet(KEY_LENGTH);
		for (int i = 0; key > 0; ++i) {
			if (key % 2 != 0) {
				bitSet.set(i);
			}
			key /= 2;
		}
		key_ = bitSet;
		assert key_.length() <= KEY_LENGTH;
	}
	
	static Key newRandomKey(Random random) {
		BitSet generatedBitSet = new BitSet(KEY_LENGTH);
		for (int i = 0; i < KEY_LENGTH; ++i) {
			if (random.nextBoolean()) {
				generatedBitSet.set(i);
			}
		}

		return new Key(generatedBitSet);
	}
	
	static Key xor(Key a, Key b) {
		BitSet newKey = a.getBitSet();
		newKey.xor(b.key_);
		return new Key(newKey);
	}
	
	/**
	 * @return distance between two key in little-endian encoding.
	 */
	BitSet calculateDistance(Key otherKey) {
		return xor(otherKey).key_;
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
		if (key_ == null) {
			if (other.key_ != null) {
				return false;
			}
		} else if (!key_.equals(other.key_)) {
			return false;
		}
		return true;
	}

	/**
	 * @return little-endian encoding of this key
	 */
	public BitSet getBitSet() {
		return (BitSet) key_.clone();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key_ == null) ? 0 : key_.hashCode());
		return result;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder(KEY_LENGTH);
		for (int i = KEY_LENGTH - 1; i >= 0; --i) {
			if (key_.get(i)) {
				strBuilder.append("1");
			} else {
				strBuilder.append("0");
			}
		}
		BigInteger bigInteger = new BigInteger(strBuilder.toString(), BINARY);

		return String.format("Key[key: %s]", bigInteger.toString(HEX));
	}

	Key xor(Key b) {
		return xor(this, b);
	}
}
