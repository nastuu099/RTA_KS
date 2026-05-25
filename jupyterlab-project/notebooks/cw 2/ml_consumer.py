from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"

print("Konsument ML Scoring uruchomiony. Oczekiwanie na transakcje...\n")

for message in consumer:
    tx = message.value
    
    # 1. Wyciągnięcie cech zgodnych z wymaganiami modelu
    amount = tx.get('amount', 0.0)
    
    # Kategoria 'elektronika' to is_electronics = 1, w przeciwnym wypadku 0
    is_electronics = 1 if tx.get('category') == 'elektronika' else 0
    
    # Domyślna wartość tx_per_minute = 5, lub zliczana na podstawie innych kryteriów
    tx_per_minute = tx.get('tx_per_minute', 5)
    
    features = {
        "amount": amount,
        "is_electronics": is_electronics,
        "tx_per_minute": tx_per_minute
    }
    
    try:
        # 2. Odpytanie serwera API (FastAPI)
        response = requests.post(API_URL, json=features)
        result = response.json()
        
        is_fraud = result.get("is_fraud", False)
        prob = result.get("fraud_probability", 0.0)
        
        # 3. Reakcja na wynik scoringu
        if is_fraud:
            alert = {
                "transaction": tx,
                "fraud_probability": prob,
                "alert_timestamp": datetime.now().isoformat()
            }
            # Wyślij powiadomienie do tematu 'alerts'
            alert_producer.send('alerts', value=alert)
            print(f"🚨 ALERT ML! ID: {tx['tx_id']} | Kwota: {amount:.2f} PLN | Prawdopodobieństwo fraudu: {prob:.1%} | Sklep: {tx['store']}")
        else:
            print(f"✔️ Transakcja OK | ID: {tx['tx_id']} | Kwota: {amount:.2f} PLN | Prawdopodobieństwo: {prob:.1%}")
            
    except Exception as e:
        print(f"❌ Błąd podczas komunikacji z API: {e}")
