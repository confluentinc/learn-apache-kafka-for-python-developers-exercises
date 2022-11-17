from confluent_kafka.admin import (AdminClient, NewTopic, 
                                   ConfigResource)
from config import config

# return True if topic exists and False if not
def topic_exists(admin, topic):
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False

# create new topic and return results dictionary
def create_topic(admin, topic):
    new_topic = NewTopic(topic, num_partitions=6, replication_factor=3) 
    result_dict = admin.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

# check max.message.bytes property
def check_max_size(admin, topic):
    resource = ConfigResource('topic', topic)
    result_dict = admin.describe_configs([resource])
    config_entries = result_dict[resource].result()
    max_size = config_entries['max.message.bytes']
    return max_size.value

# set max.message.bytes for topic
def set_max_size(admin, topic, max_k):
    config_dict = {'max.message.bytes': str(max_k*1024)}
    resource = ConfigResource('topic', topic, config_dict)
    result_dict = admin.alter_configs([resource])
    result_dict[resource].result()

if __name__ == '__main__':
    # Create Admin client
    admin = AdminClient(config)
    topic_name = 'orders'
    max_msg_k = 50
    # Create topic if it doesn't exist
    if not topic_exists(admin, topic_name):
        create_topic(admin, topic_name)
    # Check max.message.bytes config and set if needed
    current_max = check_max_size(admin, topic_name)
    if current_max != str(max_msg_k * 1024):
        print(f'Topic, {topic_name} max.message.bytes is {current_max}.')
        set_max_size(admin, topic_name, max_msg_k)

    # Verify config was set 
    new_max = check_max_size(admin, topic_name)
    print(f'Now max.message.bytes for topic {topic_name} is {new_max}')
