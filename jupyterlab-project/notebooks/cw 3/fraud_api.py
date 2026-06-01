from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app = FastAPI(title="Fraud Detection API — Isolation Forest")

model = pickle.load(open('fraud_model_if.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

@app.post("/score")
def score(tx: Transaction):
    X = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    prediction     = model.predict(X)[0]           # +1 lub -1
    anomaly_score  = model.decision_function(X)[0]  # ujemny = bardziej podejrzany

    # Normalizujemy score do przedziału [0, 1] — dla spójności z Ćw. 2
    # decision_function typowo zwraca wartości z zakresu [-0.5, 0.5]
    fraud_probability = float(np.clip(0.5 - anomaly_score, 0.0, 1.0))

    return {
        "is_fraud":          bool(prediction == -1),
        "fraud_probability": round(fraud_probability, 4),
        "model":             "isolation_forest",
    }

@app.get("/health")
def health():
    return {"status": "ok"}
