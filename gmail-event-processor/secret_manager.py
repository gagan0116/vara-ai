from google.cloud import secretmanager
import json

PROJECT_ID = "vara-483300"
import os


def access_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("utf-8")

def load_gmail_token():

    return json.loads(access_secret("gmail_token"))

def load_nova_api_key():
    return access_secret("gemini-api-key") # Keep GCP secret name same to avoid touching deployment infra
