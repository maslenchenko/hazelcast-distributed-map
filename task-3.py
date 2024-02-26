import hazelcast

client = hazelcast.HazelcastClient(
    cluster_members=[
        '127.0.0.1:5701',
        '127.0.0.1:5702',
        '127.0.0.1:5703'
    ]
)

distributed_map = client.get_map('my-distributed-map').blocking()

for i in range(1000):
    distributed_map.put(i, f'value-{i}')
