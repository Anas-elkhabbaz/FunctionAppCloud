import logging
import os
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import pyodbc
from datetime import datetime, timedelta
import azure.functions as func  # Import pour gérer les Azure Functions

# Configuration des clients et des variables globales
connection_string = "DefaultEndpointsProtocol=https;AccountName=blobscan1;AccountKey=X6mTfjDomZS4Iptfowl0Cp00jRdCdVX9h6SjmmXNorEDHHpPnxOWNkTUlaLlTBp9JRskaLRctqOq+AStQtlpnQ==;EndpointSuffix=core.windows.net"
container_name = "ocr"

computer_vision_key = "49uJ4s8ljnkxw4gaoJP8nt2DYLPu8k4KU8agl8s6tvmGg5CVaEVoJQQJ99ALAC5RqLJXJ3w3AAALACOGC1qH"
computer_vision_endpoint = "https://scanvision2.cognitiveservices.azure.com/"
credential = AzureKeyCredential(computer_vision_key)
client = DocumentAnalysisClient(computer_vision_endpoint, credential)

sql_connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:ocrser.database.windows.net,1433;Database=OCRDatabase1;Uid=anas;Pwd=Othmane2003;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"

# Fonction pour extraire le texte d'une URL de blob
def extract_text(blob_url: str) -> str:
    logging.info(f"Analyzing document from URL: {blob_url}")
    try:
        poller = client.begin_analyze_document_from_url("prebuilt-document", blob_url)
        result = poller.result()
        extracted_text = ""
        for page in result.pages:
            for line in page.lines:
                extracted_text += line.content + "\n"
        return extracted_text
    except Exception as e:
        logging.error(f"Error analyzing document: {str(e)}")
        return ""

# Fonction pour insérer du texte extrait dans la base de données
def insert_to_db(document_id: str, text_content: str):
    try:
        with pyodbc.connect(sql_connection_string) as conn:
            cursor = conn.cursor()
            query = "INSERT INTO ExtractedText (DocumentID, TextContent) VALUES (?, ?)"
            cursor.execute(query, document_id, text_content)
            conn.commit()
            logging.info(f"Text for DocumentID {document_id} inserted into database.")
    except Exception as e:
        logging.error(f"Error inserting text into the database: {str(e)}")

# Fonction pour générer un SAS Token
def generate_sas_token(blob_service_client, container_name, blob_name):
    try:
        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)  # 1-hour validity
        )
        return f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    except Exception as e:
        logging.error(f"Error generating SAS token: {str(e)}")
        return None

# Fonction principale pour le déclencheur Blob
def main(myblob: func.InputStream):
    logging.info("func start")
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # Read the blob content
    blob_content = myblob.read()
    logging.info(f"Blob Content: {blob_content}")
    try:
        # Obtenir les informations du fichier déclencheur
        filename = myblob.name.split("/")[-1]
        logging.info(f"Processing blob: {filename}, Size: {myblob.length} bytes")

        # Créer un BlobServiceClient pour générer un SAS URL
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Générer l'URL SAS
        blob_url = generate_sas_token(blob_service_client, container_name, filename)
        if not blob_url:
            logging.error("Failed to generate SAS URL.")
            return

        # Extraire le texte du fichier
        extracted_text = extract_text(blob_url)
        if not extracted_text:
            logging.error(f"Failed to extract text from blob: {filename}")
            return

        # Insérer dans la base de données
        insert_to_db(filename, extracted_text)
        logging.info(f"Text extracted and saved for blob: {filename}")

    except Exception as e:
        logging.error(f"Unexpected error processing blob: {str(e)}", exc_info=True)
