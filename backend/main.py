from fastapi import FastAPI, Request, HTTPException
import os

app = FastAPI()

@app.get("/v1/health")
def health():
    return {"status": "ok"}

@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    if not secret:
        raise HTTPException(status_code=400, detail="Missing webhook secret")

    event = await request.json()
    etype = event.get("type", "")
    if etype == "checkout.session.completed":
        # TODO: provision достъп, CRM запис, имейл
        pass
    return {"received": True}

