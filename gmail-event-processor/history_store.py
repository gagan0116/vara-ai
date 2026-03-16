from google.cloud import firestore
import os



db = firestore.Client()
DOC = db.collection("gmail_state").document("history")

def load_history_id() -> int:

    doc = DOC.get()
    if not doc.exists:
        # First run — Gmail watch start point
        return 0
    return int(doc.to_dict()["last_history_id"])

def save_history_id(history_id: int):
    DOC.set({"last_history_id": history_id})
