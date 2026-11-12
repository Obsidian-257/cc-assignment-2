import csv
import io
import json
import logging
import os
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# 1A. BLOB TRIGGER (Production / Azure via Event Grid)
@app.blob_trigger(
    arg_name="blob",
    path="uploads/{name}",
    connection="AzureWebJobsStorage",
    source=func.BlobSource.EVENT_GRID
)
@app.durable_client_input(client_name="client")
async def blob_starter(blob: func.InputStream, client: df.DurableOrchestrationClient):
    logging.info(f"Event Grid Blob trigger fired for: {blob.name}")
    csv_text = blob.read().decode("utf-8")
    payload = {"filename": blob.name, "csv_text": csv_text}
    instance_id = await client.start_new("process_csv_orchestrator", client_input=payload)
    logging.info(f"Started orchestration {instance_id} for {blob.name}")


# 1B. HTTP TRIGGER (Local Testing Starter)
@app.route(route="start-local")
@app.durable_client_input(client_name="client")
async def http_starter(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    filename = req.params.get("file", "sample_sales.csv")
    if not os.path.exists(filename):
        return func.HttpResponse(f"File {filename} not found locally.", status_code=400)

    with open(filename, "r", encoding="utf-8") as f:
        csv_text = f.read()

    payload = {"filename": filename, "csv_text": csv_text}
    instance_id = await client.start_new("process_csv_orchestrator", client_input=payload)
    logging.info(f"Started local orchestration {instance_id}")
    return client.create_check_status_response(req, instance_id)


# 2. ORCHESTRATOR (Fan-Out / Fan-In)
@app.orchestration_trigger(context_name="context")
def process_csv_orchestrator(context: df.DurableOrchestrationContext):
    payload = context.get_input()
    filename = payload["filename"]
    csv_text = payload["csv_text"]

    # Parse CSV rows (skip header)
    reader = csv.DictReader(io.StringIO(csv_text))
    records = list(reader)

    # Split into chunks of 250 records each
    chunk_size = 250
    chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]

    # FAN-OUT: Create a list of tasks WITHOUT 'yield' inside the loop
    parallel_tasks = [context.call_activity("process_chunk", chunk) for chunk in chunks]

    # FAN-IN: Yield all tasks together so they run concurrently
    results = yield context.task_all(parallel_tasks)

    # Aggregate results
    total_records = sum(r["count"] for r in results)
    total_revenue = round(sum(r["revenue"] for r in results), 2)

    summary = {
        "file": filename,
        "chunks_processed": len(chunks),
        "total_records": total_records,
        "total_revenue": total_revenue,
        "average_transaction": round(total_revenue / total_records, 2) if total_records else 0
    }

    # Write summary to 'summaries' container
    yield context.call_activity("save_summary", summary)
    return summary


# 3. ACTIVITY: Process a single chunk
@app.activity_trigger(input_name="chunk")
def process_chunk(chunk: list) -> dict:
    count = len(chunk)
    revenue = sum(float(row["amount"]) for row in chunk)
    return {"count": count, "revenue": revenue}


# 4. ACTIVITY: Save summary JSON to Blob Storage
@app.activity_trigger(input_name="summary")
def save_summary(summary: dict) -> str:
    conn_str = os.environ["AzureWebJobsStorage"]
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client("summaries")
    if not container_client.exists():
        container_client.create_container()

    clean_name = os.path.basename(summary["file"]).replace(".csv", "_summary.json")
    blob_client = container_client.get_blob_client(clean_name)
    blob_client.upload_blob(json.dumps(summary, indent=2), overwrite=True)
    return f"Saved summaries/{clean_name}"