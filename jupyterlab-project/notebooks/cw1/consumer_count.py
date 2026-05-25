from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

print("Uruchomiono konsumenta zliczającego statystyki per sklep...\n")
for message in consumer:
    tx = message.value
    store = tx.get('store', 'Nieznany')
    amount = tx.get('amount', 0.0)
    
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "="*60)
        print(f" PODSUMOWANIE (Suma wiadomości: {msg_count})")
        print("="*60)
        print(f" {'Sklep':<15} | {'Liczba transakcji':<18} | {'Łączny obrót (PLN)':<18}")
        print("-"*60)
        for s in sorted(store_counts.keys()):
            print(f" {s:<15} | {store_counts[s]:<18} | {total_amount[s]:<18.2f}")
        print("="*60 + "\n")
