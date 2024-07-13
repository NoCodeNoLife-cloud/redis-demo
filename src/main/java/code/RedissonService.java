package code;

// Copyright (c) 2024, NoCodeNoLife-cloud. All rights reserved.
// Author: NoCodeNoLife-cloud
// stay hungry，stay foolish
import org.redisson.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class RedissonService {
	private final RedissonClient redissonClient;

	public RedissonService(@Autowired RedissonClient redissonClient) {
		this.redissonClient = redissonClient;
	}

	/**
	 * Adds a value to an RBucket with the specified duration.
	 *
	 * @param bucketKey the key of the RBucket
	 * @param value     the value to be added
	 * @param duration  the duration for which the value should be stored
	 */
	public <T> void addRBucketValue(String bucketKey, T value, Duration duration) {
		RBucket<T> rBucket = redissonClient.getBucket(bucketKey);
		rBucket.set(value, duration);
	}

	/**
	 * Adds a value to an RBucket with the specified key.
	 *
	 * @param bucketKey the key of the RBucket
	 * @param value     the value to be added
	 */
	public <T> void addRBucketValue(String bucketKey, T value) {
		// Get the RBucket object from the RedissonClient
		RBucket<T> rBucket = redissonClient.getBucket(bucketKey);

		// Set the value in the RBucket
		rBucket.set(value);
	}

	/**
	 * Retrieves the value stored in an RBucket identified by the given key.
	 *
	 * @param bucketKey the key of the RBucket
	 *
	 * @return the value stored in the RBucket
	 */
	@SuppressWarnings("unchecked")
	public <T> T getRBucketValue(String bucketKey) {
		return (T) redissonClient.getBucket(bucketKey).get();
	}

	/**
	 * Adds multiple key-value pairs to a Redisson RMap with the specified duration.
	 *
	 * @param mapName  The name of the RMap.
	 * @param values   The key-value pairs to be added.
	 * @param duration The duration for which the key-value pairs should be stored.
	 */
	public <K, V> void addRMapValue(String mapName, Map<K, V> values, Duration duration) {
		// Get the RMap from RedissonClient by the given mapName
		RMap<K, V> rMap = redissonClient.getMap(mapName);

		// Add all the key-value pairs to the RMap
		rMap.putAll(values);

		// Set the expiration of the RMap to the specified duration
		rMap.expire(duration);
	}

	/**
	 * Adds key-value pairs to a Redisson RMap.
	 *
	 * @param mapName The name of the RMap.
	 * @param values  The key-value pairs to be added.
	 */
	public <K, V> void addRMapValue(String mapName, Map<K, V> values) {
		// Get the RMap from RedissonClient by the given mapName
		RMap<K, V> rMap = redissonClient.getMap(mapName);

		// Add all the key-value pairs to the RMap
		rMap.putAll(values);
	}

	/**
	 * Retrieves the value associated with the given key from a Redisson RMap.
	 *
	 * @param mapName The name of the RMap.
	 * @param key     The key whose value needs to be retrieved.
	 *
	 * @return The value associated with the given key.
	 */
	@SuppressWarnings("unchecked")
	public <K, V> V getRMapValueByKey(String mapName, K key) {
		return (V) redissonClient.getMap(mapName).get(key);
	}

	/**
	 * Retrieves all the keys from the RedissonClient.
	 *
	 * @return An iterable containing all the keys.
	 */
	public Iterable<String> getAllKeys() {
		// Get the keys from the RedissonClient
		return redissonClient.getKeys().getKeys();
	}

	/**
	 * Adds multiple values to a Redisson RList with the specified duration.
	 *
	 * @param listName The name of the RList.
	 * @param values   The values to be added.
	 * @param duration The duration for which the values should be stored.
	 */
	public <T> void addRListValue(String listName, List<T> values, Duration duration) {
		// Get the RList from RedissonClient by the given listName
		RList<T> rList = redissonClient.getList(listName);

		// Add all the values to the RList
		rList.addAll(values);

		// Set the expiration of the RList to the specified duration
		rList.expire(duration);
	}

	/**
	 * Adds a list of values to a Redisson RList.
	 *
	 * @param listName The name of the RList.
	 * @param values   The list of values to be added.
	 */
	public <T> void addRListValue(String listName, List<T> values) {
		// Get the RList from RedissonClient by the given listName
		RList<T> rList = redissonClient.getList(listName);

		// Add all the values to the RList
		rList.addAll(values);
	}

	/**
	 * Retrieves an element from a Redisson RList by index.
	 *
	 * @param listName The name of the RList.
	 * @param index    The index of the element to retrieve.
	 *
	 * @return The element at the specified index.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getRListByIndex(String listName, int index) {
		return (T) redissonClient.getList(listName).get(index);
	}

	/**
	 * Retrieves the list of values stored in a Redisson RList by the given listName.
	 *
	 * @param listName The name of the RList.
	 *
	 * @return The list of values stored in the RList.
	 */
	public <T> RList<T> getRListValue(String listName) {
		return redissonClient.getList(listName);
	}

	/**
	 * Adds a set of values to a Redisson RSet with the specified duration.
	 *
	 * @param setName  The name of the RSet.
	 * @param values   The set of values to be added.
	 * @param duration The duration for which the set should be stored.
	 * @param <T>      The type of the values in the set.
	 */
	public <T> void addRSetValue(String setName, Set<T> values, Duration duration) {
		// Get the RSet from RedissonClient by the given setName
		RSet<T> rSet = redissonClient.getSet(setName);

		// Add all the values to the RSet
		rSet.addAll(values);

		// Set the expiration of the RSet to the specified duration
		rSet.expire(duration);
	}

	/**
	 * Adds a set of values to a Redisson RSet without specifying a duration.
	 *
	 * @param setName The name of the RSet.
	 * @param values  The set of values to be added.
	 * @param <T>     The type of the values in the set.
	 */
	public <T> void addRSetValue(String setName, Set<T> values) {
		// Get the RSet from RedissonClient by the given setName
		RSet<T> rSet = redissonClient.getSet(setName);

		// Add all the values to the RSet
		rSet.addAll(values);
	}

	/**
	 * Retrieves the value associated with the given key from a Redisson RSet.
	 *
	 * @param setName The name of the RSet.
	 * @param <T>     The type of the value in the RSet.
	 *
	 * @return The value associated with the given key.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getRSetValue(String setName) {
		return (T) redissonClient.getSet(setName);
	}

	/**
	 * Adds a sorted set of values to a Redisson RSet with the specified duration.
	 *
	 * @param setName  The name of the RSet.
	 * @param values   The sorted set of values to be added.
	 * @param duration The duration for which the sorted set should be stored.
	 * @param <T>      The type of the values in the set.
	 */
	public <T> void addRSortedSetValue(String setName, Set<T> values, Duration duration) {
		// Get the RSet from RedissonClient by the given setName
		RSet<T> rSet = redissonClient.getSet(setName);

		// Add all the values to the RSet
		rSet.addAll(values);

		// Set the expiration of the RSet to the specified duration
		rSet.expire(duration);
	}

	/**
	 * Adds a sorted set of values to a Redisson RSet.
	 *
	 * @param setName The name of the RSet.
	 * @param values  The sorted set of values to be added.
	 * @param <T>     The type of the values in the set.
	 */
	public <T> void addRSortedSetValue(String setName, Set<T> values) {
		// Get the RSet from RedissonClient by the given setName
		RSet<T> rSet = redissonClient.getSet(setName);

		// Add all the values to the RSet
		rSet.addAll(values);
	}

	/**
	 * Retrieves a Redisson RSet value.
	 *
	 * @param <T>     The type of the values in the set.
	 * @param setName The name of the RSet.
	 *
	 * @return The RSet value.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getRSortedSetValue(String setName) {
		// Get the RSet from RedissonClient by the given setName
		RSet<T> rSet = redissonClient.getSet(setName);

		// Return the RSet value
		return (T) rSet;
	}

	/**
	 * Retrieves a Redisson RBloomFilter instance.
	 *
	 * @param filterName         The name of the RBloomFilter.
	 * @param expectedInsertions The expected number of insertions.
	 * @param falseProbability   The desired false positive probability.
	 * @param duration           The duration for which the RBloomFilter should be valid.
	 *
	 * @return The RBloomFilter instance.
	 */
	public RBloomFilter<Object> getRBloomFilter(String filterName, int expectedInsertions, int falseProbability, Duration duration) {
		// Get the RBloomFilter from RedissonClient by the given filterName
		RBloomFilter<Object> rBloomFilter = redissonClient.getBloomFilter(filterName);

		// Initialize the RBloomFilter with the given parameters
		rBloomFilter.tryInit(expectedInsertions, falseProbability);

		// Set the expiration duration for the RBloomFilter
		rBloomFilter.expire(duration);

		// Return the RBloomFilter instance
		return rBloomFilter;
	}

	/**
	 * Retrieves or creates a distributed AtomicLong with a specific lock key and sets its expiration duration.
	 *
	 * @param lockKey  The key associated with the AtomicLong.
	 * @param duration The duration for which the AtomicLong should be valid.
	 *
	 * @return The value of the AtomicLong before incrementing.
	 */
	public long getAtomicLong(String lockKey, Duration duration) {
		// Retrieve or create a distributed AtomicLong with the specified lock key
		RAtomicLong atomicLong = redissonClient.getAtomicLong(lockKey);

		// Set the expiration duration for the AtomicLong
		atomicLong.expire(duration);

		// Return the current value of the AtomicLong and increment it by 1
		return atomicLong.getAndIncrement();
	}

	/**
	 * Retrieves or creates a distributed AtomicLong with a specific lock key and increments it by 1.
	 *
	 * @param lockKey The key associated with the AtomicLong.
	 *
	 * @return The value of the AtomicLong before incrementing.
	 */
	public long getAtomicLong(String lockKey) {
		// Retrieve or create a distributed AtomicLong with the specified lock key
		RAtomicLong atomicLong = redissonClient.getAtomicLong(lockKey);

		// Return the current value of the AtomicLong and increment it by 1
		return atomicLong.getAndIncrement();
	}

	/**
	 * Shuts down the Redisson client.
	 * <p>
	 * This method is used to gracefully shut down the Redisson client, releasing any
	 * resources it holds. It should be called when the application is being terminated
	 * or when it is no longer needed.
	 */
	public void shutDown() {
		// Shut down the Redisson client
		redissonClient.shutdown();
	}
}