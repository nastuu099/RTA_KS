from fastapi import FastAPI
from pydantic import BaseModel
import pickle, numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

@app.post("/score")
def score(tx: Transaction):
    # Przygotowanie cech do modelu (tablica 2D numpy)
    features = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    
    # Predykcja binarna (0 lub 1)
    pred = model.predict(features)[0]
    
    # Prawdopodobieństwo przynależności do klasy fraud (klasa 1)
    prob = model.predict_proba(features)[0][1]
    
    return {
        "is_fraud": bool(pred),
        "fraud_probability": float(prob)
    }

# Endpoint zdrowia (Zadanie z pracy domowej)
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "model_loaded": model is not None
    }
