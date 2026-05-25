from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono konsumenta filtrującego duże transakcje (>1000)...\n")
for message in consumer:
    tx = message.value
    if tx.get('amount', 0) > 1000:
        print(f"🚨 ALERT: Duża transakcja! ID: {tx['tx_id']} | Kwota: {tx['amount']:.2f} PLN | Kategoria: {tx['category']} | Sklep: {tx['store']}")
