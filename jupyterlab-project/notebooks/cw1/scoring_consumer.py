from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1 (amount > 3000)')
        
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2 (elektronika & amount > 1500)')
        
    hour = tx.get('hour')
    if hour is None and 'timestamp' in tx:
        try:
            dt = datetime.fromisoformat(tx['timestamp'])
            hour = dt.hour
        except:
            pass
            
    if hour is not None and hour < 6:
        score += 2
        rules.append('R3 (hour < 6)')
        
    return score, rules

print("Uruchomiono konsumenta scoringowego...\n")
for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    if score >= 3:
        alert = {
            'tx_id': tx.get('tx_id'),
            'amount': tx.get('amount'),
            'store': tx.get('store'),
            'category': tx.get('category'),
            'hour': tx.get('hour'),
            'fraud_score': score,
            'triggered_rules': rules,
            'timestamp': datetime.now().isoformat()
        }
        alert_producer.send('alerts', value=alert)
        print(f"🚨 [ALERT] Transakcja {tx['tx_id']} jest podejrzana! Score: {score} | Reguły: {rules} | Kwota: {tx['amount']:.2f} PLN | Godzina: {tx['hour']}")
