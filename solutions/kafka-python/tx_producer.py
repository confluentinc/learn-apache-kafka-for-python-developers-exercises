from confluent_kafka import Producer
from config import config
import json


order_events = [
    {'order_id': '101', 'product_id': 'xyz123', 'quantity': 3},
    {'order_id': '102', 'product_id': 'abc254', 'quantity': 1},
    {'order_id': '103', 'product_id': 'xyz789', 'quantity': 5},
    {'order_id': '104', 'product_id': 'pdq001', 'quantity': 2}]

inventory_events = [
    {'product_id': 'xyz123', 'quantity': -3},
    {'product_id': 'abc254', 'quantity': -1},
    {'product_id': 'xyz789', 'quantity': -5},
    {'product_id': 1001, 'quantity': -2}]

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
        raise Exception(err)
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to topic {event.topic()}.')

if __name__ == '__main__':
    config['transactional.id'] = 'order_inventory_txn'
    producer = Producer(config)
    producer.init_transactions()
    for i in range(4):
        producer.begin_transaction()
        try:
            # produce to orders topic
            ord = json.dumps(order_events[i])
            ord_key = order_events[i]['order_id']
            producer.produce('orders', ord, ord_key, on_delivery=callback)
            
            # produce to inventory topic
            inv = json.dumps(inventory_events[i])
            inv_key = inventory_events[i]['product_id']
            producer.produce('inventory', inv, inv_key, on_delivery=callback)
            
            producer.flush()
            producer.commit_transaction()
        except Exception as e:
            print(f'Transaction aborted! - {str(e)}')
            producer.abort_transaction()

