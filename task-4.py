import threading
import hazelcast

def increment_map_value(client, lock_type, iterations=10_000):
    hazelcast_map = client.get_map('my-distributed-map').blocking()
    
    key = 'key'

    hazelcast_map.put(key, 0)

    for _ in range(iterations):
        if lock_type == 'pessimistic':
            hazelcast_map.lock(key)
            try:
                value = hazelcast_map.get(key)
                hazelcast_map.put(key, value + 1)
            finally:
                hazelcast_map.unlock(key)
        elif lock_type == 'optimistic':
            while True:
                value = hazelcast_map.get(key)
                if hazelcast_map.replace_if_same(key, value, value + 1):
                    break
        else:
            value = hazelcast_map.get(key)
            hazelcast_map.put(key, value + 1)

def run_client(lock_type):
    client = hazelcast.HazelcastClient()
    increment_map_value(client, lock_type)
    client.shutdown()

def start_simulation(lock_type):
    threads = []
    for _ in range(3):
        thread = threading.Thread(target=run_client, args=(lock_type,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    client = hazelcast.HazelcastClient()
    final_value = client.get_map('my-distributed-map').blocking().get('key')
    client.shutdown()
    return final_value

final_value_without_lock = start_simulation(None)
print(f'Final value without locks: {final_value_without_lock}')

final_value_with_pessimistic_lock = start_simulation('pessimistic')
print(f'Final value with pessimistic locks: {final_value_with_pessimistic_lock}')

final_value_with_optimistic_lock = start_simulation('optimistic')
print(f'Final value with optimistic locks: {final_value_with_optimistic_lock}')

print('All simulations completed.')
